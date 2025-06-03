package com.linkedin.hoptimator.operator.trigger;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sYamlApi;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import com.linkedin.hoptimator.util.Template;


/**
 * Launches Jobs when TableTriggers are fired.
 * <p>
 * TableTriggers maintain a timestamp and a watermark. The timestamp captures
 * the time at which a matching event occured, which could be far in the past.
 * The watermark records the last timestamp for which a corresponding job has
 * successfully completed, and is thus always older than or equal to the
 * timestamp.
 * <p>
 * At steady-state, a trigger can be in one of two states:
 * <p>
 * 1. Timestamp and watermark are the same: trigger has been fired and the
 *    corresponding job has successfully completed.
 * 2. Watermark is older than the timestamp: trigger has been fired, but a new
 *    corresponding job has not yet successfully completed.
 * <p>
 * At a high level, the reconciler checks whether the watermark is old and
 * creates a Job accordingly. If a Job already exists, we just wait for it
 * to complete. Once completed, we update the watermark to match the specific
 * timestamp that caused the Job to run.
 * <p>
 * Only one Job runs at a time, which means a trigger may be fired many times
 * before a Job successfully completes. Rather than fall behind, we pass the
 * current watermark and timestamp to each Job (e.g. via environment variables).
 * The Job itself must decide what to do based on this window of time.
 * Generally, a larger window means more work to do.
 * <p>
 */
public class TableTriggerReconciler implements Reconciler {
  private static final Logger log = LoggerFactory.getLogger(TableTriggerReconciler.class);
  private static final String TRIGGER_KEY = "trigger";
  private static final String TRIGGER_TIMESTAMP_KEY = "triggerTimestamp";

  private final K8sContext context;
  private final K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi;
  private final K8sApi<V1Job, V1JobList> jobApi;
  private final K8sYamlApi yamlApi;

  private TableTriggerReconciler(K8sContext context) {
    this(context, new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS),
        new K8sApi<>(context, K8sApiEndpoints.JOBS),
        new K8sYamlApi(context));
  }

  TableTriggerReconciler(K8sContext context,
      K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi,
      K8sApi<V1Job, V1JobList> jobApi, K8sYamlApi yamlApi) {
    this.context = context;
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
      V1alpha1TableTrigger object = tableTriggerApi.get(namespace, name);

      if (object == null) {
        log.info("Object {} deleted. Skipping.", name);
        return new Result(false);
      }

      V1alpha1TableTriggerStatus status = object.getStatus();
      if (status == null || status.getTimestamp() == null) {
        log.info("Trigger {} has not been fired yet. Skipping.", name);
        return new Result(false);
      }

      log.info("TableTrigger {} was last fired at {}.", name, status.getTimestamp());

      if (object.getSpec().getYaml() == null) {
        log.info("Trigger {} has no Job YAML. Will take no action.", name);
        return new Result(false);
      }

      // Find corresponding Job.
      Collection<V1Job> jobs = jobApi.select(TRIGGER_KEY + " = " + name);
      V1Job job = jobs.stream().findFirst().orElse(null);  // assume only one job.

      boolean shouldFire = false;
      if (status.getTimestamp() != null && status.getWatermark() != null) {
        // Only fire if the new timestamp is after the watermark
        shouldFire = status.getTimestamp().isAfter(status.getWatermark());
      } else if (status.getTimestamp() != null) {
        // If this is the first trigger (no watermark yet), fire
        shouldFire = true;
      }

      if (job == null && shouldFire) {
        log.info("Launching Job for TableTrigger {}. ", name);
        createJob(object);
        return new Result(true, pendingRetryDuration());
      } else if (job != null && job.getStatus() != null && job.getStatus().getConditions() != null) {
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
          if (job.getMetadata().getAnnotations() == null || job.getMetadata().getAnnotations()
              .get(TRIGGER_TIMESTAMP_KEY) == null) {
            log.error("Job {} has no timestamp annotation. Unable to advance the watermark.", name);
          } else {
            String watermark = job.getMetadata().getAnnotations().get(TRIGGER_TIMESTAMP_KEY);
            status.setWatermark(OffsetDateTime.parse(watermark));
            tableTriggerApi.updateStatus(object, status);
            log.info("Trigger {} watermark advanced to {}.", name, watermark);
          }
          jobApi.delete(job);
          return new Result(true);  // retry
        } else {
          log.info("Job for TableTrigger {} still running from a previous trigger event.", name);
          return new Result(true, pendingRetryDuration());  // retry later
        }
      } else {
        log.info("Job for TableTrigger {} has no status yet.", name);
        if (shouldInvokeTableTrigger()) {
          status.setTimestamp(OffsetDateTime.now()); // updating the timestamp will trigger the job creation
          return new Result(true);  // retry now
        }
        return new Result(true, pendingRetryDuration());  // retry later
      }
    } catch (Exception e) {
      log.error("Encountered exception while reconciling TableTrigger {}.", name, e);
      return new Result(true, failureRetryDuration());
    }
  }

  // This method can be overridden to add custom logic for when to invoke the TableTrigger.
  public boolean shouldInvokeTableTrigger() {
    int currentMinute = OffsetDateTime.now().getMinute();
    // dummy logic: invoke every 5 minutes
    return currentMinute % 5 == 0;
  }

  private void createJob(V1alpha1TableTrigger trigger) throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("trigger", trigger.getMetadata().getName())
        .with("schema", trigger.getSpec().getSchema())
        .with("table", trigger.getSpec().getTable())
        .with("timestamp", trigger.getStatus().getTimestamp().toString())
        .with("watermark", Optional.ofNullable(trigger.getStatus().getWatermark())
            .map(x -> x.toString()).orElse(null));
    String yaml = new Template.SimpleTemplate(trigger.getSpec().getYaml()).render(env);
    Map<String, String> annotations = new HashMap<>();
    annotations.put(TRIGGER_KEY, trigger.getMetadata().getName());
    annotations.put(TRIGGER_TIMESTAMP_KEY, trigger.getStatus().getTimestamp().toString());
    Map<String, String> labels = new HashMap<>();
    labels.put(TRIGGER_KEY, trigger.getMetadata().getName());
    yamlApi.createWithAnnotationsAndLabels(yaml, annotations, labels);
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
}

