package com.linkedin.hoptimator.operator.trigger;

import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.linkedin.hoptimator.DataChange;
import com.linkedin.hoptimator.InputFrontierSource;
import com.linkedin.hoptimator.jdbc.DeployerUtils;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sTriggerJobs;
import com.linkedin.hoptimator.k8s.K8sYamlApi;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import com.linkedin.hoptimator.util.planner.HoptimatorJdbcSchema;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobCondition;
import io.kubernetes.client.openapi.models.V1JobList;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
  private final FrontierSourceResolver frontierSourceResolver;

  private TableTriggerReconciler(K8sContext context) {
    this(new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS),
        new K8sApi<>(context, K8sApiEndpoints.JOBS),
        new K8sYamlApi(context),
        frontierSourceResolver(context.connection()));
  }

  TableTriggerReconciler(K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi,
      K8sApi<V1Job, V1JobList> jobApi, K8sYamlApi yamlApi) {
    this(tableTriggerApi, jobApi, yamlApi, (catalog, schema) -> null);
  }

  TableTriggerReconciler(K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi,
      K8sApi<V1Job, V1JobList> jobApi, K8sYamlApi yamlApi,
      FrontierSourceResolver frontierSourceResolver) {
    this.tableTriggerApi = tableTriggerApi;
    this.jobApi = jobApi;
    this.yamlApi = yamlApi;
    this.frontierSourceResolver = frontierSourceResolver;
  }

  /**
   * Production resolver: unwraps the operator's Calcite connection to the input {@code Database}'s
   * {@link HoptimatorJdbcSchema} and returns its {@link InputFrontierSource} capability, or null when
   * the input has no database or its driver surfaces no frontier capability. This is the seam that
   * replaces per-source trigger controllers: the capability hangs off the schema the driver already
   * builds, so per-cluster connection config is inherent.
   */
  private static FrontierSourceResolver frontierSourceResolver(HoptimatorConnection connection) {
    return (catalog, schema) -> {
      HoptimatorJdbcSchema jdbcSchema = DeployerUtils.jdbcSchema(catalog, schema, connection, log);
      return jdbcSchema == null ? null : jdbcSchema.inputFrontierSource().orElse(null);
    };
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

      // Ask the input's Database schema how far this input is complete in data time (the
      // InputFrontierSource capability). Null means the input is not frontier-driven -> the trigger
      // is cron-/manually-driven. This is the seam that replaces per-source trigger controllers.
      OffsetDateTime frontier = inputFrontier(object);

      if (status == null && frontier == null && object.getSpec().getSchedule() == null) {
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

      // Data-availability firing: when a source reports a data-time frontier for the input, advance
      // the cursor to it and launch the Job over the newly-available window [watermark, timestamp].
      // The frontier is an optimistic signal — the source has seen data through it, not a guarantee
      // that everything at or before it has arrived — so late writes behind the cursor are healed
      // separately via changesSince/backfill. Gated on job == null (like cron) so we process one
      // window at a time. Uniform across every source: the source supplies a frontier; this
      // reconciler owns the cursor and Job launching.
      if (job == null && frontier != null
          && (status.getTimestamp() == null || frontier.isAfter(status.getTimestamp()))) {
        log.info("Advancing TableTrigger {} to input frontier {}.", name, frontier);
        status.setTimestamp(frontier);
        tableTriggerApi.updateStatus(object, status);
        return new Result(true);
      }

      // Late-change repair: when a source reports a change that landed behind the cursor (a late or
      // out-of-order write to already-processed history), replay that data-time window as a one-off
      // backfill Job — which never moves the cursor. Consumed in arrival order via the internal
      // lateWatermark. The user-facing watermark stays the monotone forward frontier.
      if (job == null
          && frontierSourceResolver.resolve(object.getSpec().getCatalog(), object.getSpec().getSchema()) != null
          && status.getWatermark() != null) {
        Result repair = maybeEnqueueLateRepair(object, status);
        if (repair != null) {
          return repair;
        }
      }

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
    V1alpha1TableTriggerStatus status = trigger.getStatus();
    return renderJob(trigger,
        status == null ? null : status.getWatermark(),
        status == null ? null : status.getTimestamp());
  }

  /**
   * Renders the trigger's Job template for an explicit output window {@code [watermark, timestamp]}.
   * Incremental fires pass the cursor ({@code status.watermark}/{@code status.timestamp}). See
   * {@link K8sTriggerJobs#render}.
   */
  private String renderJob(V1alpha1TableTrigger trigger, OffsetDateTime watermark,
      OffsetDateTime timestamp) throws SQLException {
    return K8sTriggerJobs.render(trigger, watermark, timestamp);
  }

  private void createJob(String yaml, V1alpha1TableTrigger trigger) throws SQLException {
    Map<String, String> annotations = new HashMap<>();
    annotations.put(TRIGGER_KEY, trigger.getMetadata().getName());
    annotations.put(TRIGGER_TIMESTAMP_KEY, trigger.getStatus().getTimestamp().toString());
    Map<String, String> labels = new HashMap<>();
    labels.put(TRIGGER_KEY, trigger.getMetadata().getName());
    List<V1OwnerReference> ownerReference;
    if (trigger.getMetadata().getOwnerReferences() != null && !trigger.getMetadata().getOwnerReferences().isEmpty()) {
      ownerReference = trigger.getMetadata().getOwnerReferences();
    } else {
      ownerReference = Collections.singletonList(new V1OwnerReference()
          .apiVersion(trigger.getApiVersion())
          .kind(trigger.getKind())
          .name(trigger.getMetadata().getName())
          .uid(trigger.getMetadata().getUid()));
    }
    yamlApi.createWithMetadata(yaml, annotations, labels, ownerReference);
  }

  private ExecutionTime scheduledExecution(V1alpha1TableTrigger object) {
    if (object.getSpec().getSchedule() == null) {
      return null;
    } else {
      CronParser parser = new CronParser(CRON_DEFINITION);
      return ExecutionTime.forCron(parser.parse(object.getSpec().getSchedule()));
    }
  }

  /**
   * Resolves the input's data-time frontier from the {@link InputFrontierSource} capability of the
   * input's {@code Database} schema, in UTC, or null when the input is not frontier-driven (the
   * trigger is then cron-/manually-driven). A source that throws is logged and skipped, so one
   * misbehaving database never blocks a trigger.
   */
  private OffsetDateTime inputFrontier(V1alpha1TableTrigger trigger) {
    V1alpha1TableTriggerSpec spec = trigger.getSpec();
    InputFrontierSource source = frontierSourceResolver.resolve(spec.getCatalog(), spec.getSchema());
    if (source == null) {
      return null;
    }
    try {
      Optional<Instant> frontier = source.frontier(spec.getTable());
      return frontier.map(t -> t.atOffset(ZoneOffset.UTC)).orElse(null);
    } catch (Exception e) {
      log.warn("InputFrontierSource for {}.{} frontier failed; skipping.",
          spec.getSchema(), spec.getTable(), e);
      return null;
    }
  }

  /**
   * Consumes the source's change stream (in arrival order, via {@code status.lateWatermark}) and, on
   * the first change whose data-time window lies behind the watermark, enqueues a one-off backfill
   * over that window — clipped to the watermark so it never runs ahead of the cursor. Returns a
   * requeue {@link Result} when it writes status (a repair enqueued or the cursor consumed), or null
   * when there is nothing to do. On first sight (no {@code lateWatermark}) it initializes the cursor
   * to now, so a freshly-created trigger reacts only to <em>future</em> late changes, not all history.
   */
  private Result maybeEnqueueLateRepair(V1alpha1TableTrigger object, V1alpha1TableTriggerStatus status)
      throws SQLException {
    V1alpha1TableTriggerSpec spec = object.getSpec();
    OffsetDateTime watermark = status.getWatermark();

    if (status.getLateWatermark() == null) {
      status.setLateWatermark(OffsetDateTime.now(ZoneOffset.UTC));
      tableTriggerApi.updateStatus(object, status);
      return new Result(true);
    }
    Instant since = status.getLateWatermark().toInstant();

    InputFrontierSource source = frontierSourceResolver.resolve(spec.getCatalog(), spec.getSchema());
    List<DataChange> changes = new ArrayList<>();
    if (source != null) {
      try {
        changes.addAll(source.changesSince(spec.getTable(), since));
      } catch (Exception e) {
        log.warn("InputFrontierSource for {}.{} changesSince failed; skipping.",
            spec.getSchema(), spec.getTable(), e);
      }
    }
    if (changes.isEmpty()) {
      return null;
    }
    changes.sort(Comparator.comparing(DataChange::arrival));

    OffsetDateTime maxArrival = status.getLateWatermark();
    for (DataChange change : changes) {
      OffsetDateTime arrival = change.arrival().atOffset(ZoneOffset.UTC);
      if (!arrival.isAfter(status.getLateWatermark())) {
        continue;  // already consumed
      }
      if (maxArrival == null || arrival.isAfter(maxArrival)) {
        maxArrival = arrival;
      }
      OffsetDateTime windowStart = change.windowStart().atOffset(ZoneOffset.UTC);
      OffsetDateTime windowEnd = change.windowEnd().atOffset(ZoneOffset.UTC);
      // Only windows (at least partly) behind the watermark need repair; ahead-of-cursor changes are
      // handled by normal forward processing. Clip the end to the watermark so the backfill never
      // runs ahead of the cursor.
      if (windowStart.isBefore(watermark)) {
        OffsetDateTime cappedEnd = windowEnd.isAfter(watermark) ? watermark : windowEnd;
        log.info("Repairing late change to TableTrigger {} over [{}, {}] via backfill (arrival {}).",
            object.getMetadata().getName(), windowStart, cappedEnd, arrival);
        // Launch the repair as a one-off backfill Job (owned by the trigger, keyed by window +
        // arrival so genuinely new late data gets a fresh Job) and forget it — the Job controller
        // owns its lifecycle. Advancing lateWatermark only after the create means a crash before the
        // status write re-detects the same change and re-creates the same (idempotent) Job.
        K8sTriggerJobs.createBackfill(yamlApi, object, windowStart, cappedEnd, arrival);
        status.setLateWatermark(arrival);
        tableTriggerApi.updateStatus(object, status);
        return new Result(true);
      }
    }
    // No repair needed in this batch; consume it so we don't re-scan the same changes.
    if (maxArrival != null && maxArrival.isAfter(status.getLateWatermark())) {
      status.setLateWatermark(maxArrival);
      tableTriggerApi.updateStatus(object, status);
      return new Result(true);
    }
    return null;
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

