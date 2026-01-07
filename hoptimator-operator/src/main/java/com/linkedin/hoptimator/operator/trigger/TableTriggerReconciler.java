package com.linkedin.hoptimator.operator.trigger;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobCondition;
import io.kubernetes.client.openapi.models.V1JobList;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sYamlApi;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import com.linkedin.hoptimator.util.Template;

import com.cronutils.parser.CronParser;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;


/**
 * Launches Jobs when TableTriggers are fired.
 *
 * TableTriggers maintain a timestamp and a watermark. The timestamp captures
 * the time at which a matching event occured, which could be far in the past.
 * The watermark records the last timestamp for which a corresponding job has
 * successfully completed, and is thus always older than or equal to the
 * timestamp.
 *
 * At steady-state, a trigger can be in one of two states:
 *
 * 1. Timestamp and watermark are the same: trigger has been fired and the
 *    corresponding job has successfully completed.
 * 2. Watermark is older than the timestamp: trigger has been fired, but a new
 *    corresponding job has not yet successfully completed.
 *
 * At a high level, the reconciler checks whether the watermark is old and
 * creates a Job accordingly. If a Job already exists, we just wait for it
 * to complete. Once completed, we update the watermark to match the specific
 * timestamp that caused the Job to run.
 *
 * Only one Job runs at a time, which means a trigger may be fired many times
 * before a Job successfully completes. Rather than fall behind, we pass the
 * current watermark and timestamp to each Job (e.g. via environment variables).
 * The Job itself must decide what to do based on this window of time.
 * Generally, a larger window means more work to do.
 *
 */
public final class TableTriggerReconciler implements Reconciler {
  private static final Logger log = LoggerFactory.getLogger(TableTriggerReconciler.class);
  static final String TRIGGER_KEY = "trigger";
  static final String TRIGGER_TIMESTAMP_KEY = "triggerTimestamp";
  static final CronDefinition CRON_DEFINITION = CronDefinitionBuilder.defineCron()
      .withMinutes().withValidRange(0, 59).withStrictRange().and()
      .withHours().withValidRange(0, 23).withStrictRange().and()
      .withDayOfMonth().withValidRange(1, 31).withStrictRange().and()
      .withMonth().withValidRange(1, 12).withStrictRange().and()
      .withDayOfWeek().withValidRange(0, 7).withMondayDoWValue(1).withIntMapping(7, 0).withStrictRange().and()
      .withSupportedNicknameHourly()
      .withSupportedNicknameDaily()
      .withSupportedNicknameWeekly()
      .withSupportedNicknameMonthly()
      .withSupportedNicknameYearly()
      .withSupportedNicknameAnnually()
      .withSupportedNicknameMidnight()
      .instance();


  private final K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi;
  private final K8sApi<V1Job, V1JobList> jobApi;
  private final K8sYamlApi yamlApi;

  private TableTriggerReconciler(K8sContext context) {
    this(new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS),
        new K8sApi<>(context, K8sApiEndpoints.JOBS),
        new K8sYamlApi(context));
  }

  TableTriggerReconciler(K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi,
      K8sApi<V1Job, V1JobList> jobApi, K8sYamlApi yamlApi) {
    this.tableTriggerApi = tableTriggerApi;
    this.jobApi = jobApi;
    this.yamlApi = yamlApi;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    try {
      V1alpha1TableTrigger object;
      try {
        object = tableTriggerApi.get(namespace, name);
      } catch (SQLException e) {
        if (e.getErrorCode() == 404) {
          log.info("Object {} deleted. Skipping.", name);
          return new Result(false);
        }
        throw e;
      }

      if (object.getSpec().getYaml() == null) {
        log.info("Trigger {} has no Job YAML. Will take no action.", name);
        return new Result(false);
      }

      if (Boolean.TRUE.equals(object.getSpec().getPaused())) {
        log.info("Trigger {} is paused. Skipping job creation.", name);
        V1alpha1TableTriggerStatus status = object.getStatus();
        if (status != null) {
          DynamicKubernetesObject expectedJob = yamlApi.objFromYaml(jobYaml(object));
          V1Job job = jobApi.getIfExists(expectedJob.getMetadata().getNamespace(),
              expectedJob.getMetadata().getName());
          if (job != null) {
            log.info("Trigger {} is paused but existing job {} is still running. Monitoring it.",
                name, job.getMetadata().getName());
            return handleExistingJob(job, status, object);
          }
        }
        return new Result(false);
      }

      V1alpha1TableTriggerStatus status = object.getStatus();
      if (status == null && object.getSpec().getSchedule() == null) {
        log.info("Trigger {} has not been fired yet. Skipping.", name);
        return new Result(false);
      } else if (status == null) {
        status = new V1alpha1TableTriggerStatus();
        object.status(status);
      }

      if (status.getTimestamp() != null) {
        log.info("TableTrigger {} was last fired at {}.", name, status.getTimestamp());
      }

      // Find corresponding Job.
      String jobYaml = jobYaml(object);
      DynamicKubernetesObject expectedJob = yamlApi.objFromYaml(jobYaml);
      V1Job job = jobApi.getIfExists(expectedJob.getMetadata().getNamespace(), expectedJob.getMetadata().getName());

      ExecutionTime scheduled = scheduledExecution(object);
      ZonedDateTime now = ZonedDateTime.now();

      if (job == null && scheduled != null && (status.getTimestamp() == null
          || status.getTimestamp().isBefore(scheduled.lastExecution(now).get().toOffsetDateTime()))) {
        log.info("Firing TableTrigger {} per cron schedule.", name);
        status.setTimestamp(scheduled.lastExecution(now).get().toOffsetDateTime());
        tableTriggerApi.updateStatus(object, status);
        return new Result(true);
      }

      // sanity check
      if (status.getTimestamp() == null && object.getStatus().getTimestamp() != null) {
        throw new IllegalStateException("Trigger has no timestamp.");
      }

      if (job == null
          && (status.getWatermark() == null || status.getTimestamp().isAfter(status.getWatermark()))) {
        log.info("Launching Job for TableTrigger {}. ", name);
        createJob(jobYaml, object);
        return new Result(true, pendingRetryDuration());
      } else if (job != null) {
        return handleExistingJob(job, status, object);
      } else if (scheduled != null) {
        log.info("TableTrigger {} sleeping until next scheduled execution.", name);
        return new Result(true, scheduled.timeToNextExecution(now).get());
      } else {
        log.info("Job for TableTrigger {} has no status yet.", name);
        return new Result(true, pendingRetryDuration());  // retry later
      }
    } catch (Exception e) {
      log.error("Encountered exception while reconciling TableTrigger {}.", name, e);
      return new Result(true, failureRetryDuration());
    }
  }

  private String jobYaml(V1alpha1TableTrigger trigger) throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("trigger", trigger.getMetadata().getName())
        .with("schema", trigger.getSpec().getSchema())
        .with("table", trigger.getSpec().getTable())
        .with("timestamp", Optional.ofNullable(trigger.getStatus().getTimestamp())
            .map(x -> x.toString()).orElse(null))
        .with("watermark", Optional.ofNullable(trigger.getStatus().getWatermark())
            .map(x -> x.toString()).orElse(null));
    return new Template.SimpleTemplate(trigger.getSpec().getYaml()).render(env);
  }

  private void createJob(String yaml, V1alpha1TableTrigger trigger) throws SQLException {
    Map<String, String> annotations = new HashMap<>();
    annotations.put(TRIGGER_KEY, trigger.getMetadata().getName());
    annotations.put(TRIGGER_TIMESTAMP_KEY, trigger.getStatus().getTimestamp().toString());
    Map<String, String> labels = new HashMap<>();
    labels.put(TRIGGER_KEY, trigger.getMetadata().getName());
    yamlApi.createWithMetadata(yaml, annotations, labels, trigger.getMetadata().getOwnerReferences());
  }

  private ExecutionTime scheduledExecution(V1alpha1TableTrigger object) {
    if (object.getSpec().getSchedule() == null) {
      return null;
    } else {
      CronParser parser = new CronParser(CRON_DEFINITION);
      return ExecutionTime.forCron(parser.parse(object.getSpec().getSchedule()));
    }
  }

  // TODO load from configuration
  protected Duration failureRetryDuration() {
    return Duration.ofMinutes(5);
  }

  // TODO load from configuration
  protected Duration pendingRetryDuration() {
    return Duration.ofMinutes(1);
  }

  public static Controller controller(K8sContext context) {
    Reconciler reconciler = new TableTriggerReconciler(context);
    return ControllerBuilder.defaultBuilder(context.informerFactory())
        .withReconciler(reconciler)
        .withName("table-trigger-controller")
        .withWorkerCount(1)
        .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1TableTrigger.class, x).build())
        .build();
  }

  void maybeUpdateJobAnnotation(V1Job job, OffsetDateTime timestamp) throws SQLException {
    Map<String, String> annotations = Objects.requireNonNull(job.getMetadata()).getAnnotations();
    if (annotations != null) {
      String existing = annotations.get(TRIGGER_TIMESTAMP_KEY);
      if (existing != null && timestamp.isAfter(OffsetDateTime.parse(existing))) {
        annotations.put(TRIGGER_TIMESTAMP_KEY, timestamp.toString());
        job.getMetadata().setAnnotations(annotations);
        jobApi.update(job);
        log.info("Updated {} in Job {} annotation to {}", TRIGGER_TIMESTAMP_KEY, job.getMetadata().getName(), timestamp);
      }
    }
  }

  private Result handleExistingJob(V1Job job, V1alpha1TableTriggerStatus status,
      V1alpha1TableTrigger trigger) throws SQLException {
    String name = trigger.getMetadata().getName();

    if (job.getStatus() != null && job.getStatus().getConditions() != null) {
      List<V1JobCondition> conditions = job.getStatus().getConditions();
      boolean failed = conditions.stream()
          .anyMatch(x -> "Failed".equals(x.getType()) && "True".equals(x.getStatus()));
      boolean complete = conditions.stream()
          .anyMatch(x -> "Complete".equals(x.getType()) && "True".equals(x.getStatus()));

      if (failed) {
        log.warn("Job {} has FAILED.", name);
        jobApi.delete(job);
        return new Result(true);  // retry
      } else if (complete) {
        log.info("Job {} completed successfully.", name);
        // We get the watermark from the job itself. We annotate the job when launching it.
        if (job.getMetadata().getAnnotations() == null
            || job.getMetadata().getAnnotations().get(TRIGGER_TIMESTAMP_KEY) == null) {
          log.error("Job {} has no timestamp annotation. Unable to advance the watermark.", name);
        } else {
          String watermark = job.getMetadata().getAnnotations().get(TRIGGER_TIMESTAMP_KEY);
          status.setWatermark(OffsetDateTime.parse(watermark));
          tableTriggerApi.updateStatus(trigger, status);
          log.info("Trigger {} watermark advanced to {}.", name, watermark);
        }
        jobApi.delete(job);
        return new Result(true);  // retry
      } else {
        if (status.getTimestamp() != null) {
          maybeUpdateJobAnnotation(job, status.getTimestamp());
        }
        log.info("Job for TableTrigger {} still running.", name);
        return new Result(true, pendingRetryDuration());  // retry later
      }
    } else {
      log.info("Job for TableTrigger {} has no status yet.", name);
      return new Result(true, pendingRetryDuration());  // retry later
    }
  }
}

