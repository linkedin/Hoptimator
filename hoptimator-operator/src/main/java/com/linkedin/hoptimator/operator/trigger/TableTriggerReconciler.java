package com.linkedin.hoptimator.operator.trigger;

import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sYamlApi;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import com.linkedin.hoptimator.util.Template;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

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
  static final String BACKFILL_KEY = "backfill";
  static final String BACKFILL_INFIX = "-bf-";
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
  private final Collection<InputWatermarkProvider> inputWatermarkProviders;

  private TableTriggerReconciler(K8sContext context) {
    this(new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS),
        new K8sApi<>(context, K8sApiEndpoints.JOBS),
        new K8sYamlApi(context),
        InputWatermarkService.providers());
  }

  TableTriggerReconciler(K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi,
      K8sApi<V1Job, V1JobList> jobApi, K8sYamlApi yamlApi) {
    this(tableTriggerApi, jobApi, yamlApi, Collections.emptyList());
  }

  TableTriggerReconciler(K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi,
      K8sApi<V1Job, V1JobList> jobApi, K8sYamlApi yamlApi,
      Collection<InputWatermarkProvider> inputWatermarkProviders) {
    this.tableTriggerApi = tableTriggerApi;
    this.jobApi = jobApi;
    this.yamlApi = yamlApi;
    this.inputWatermarkProviders = inputWatermarkProviders;
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

      // Ask any registered source integration how far this input is complete in data time (the
      // InputWatermarkProvider SPI). Null means no provider handles the input -> the trigger is
      // cron-/manually-driven. This is the seam that replaces per-source trigger controllers.
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

      // A backfill is a one-off Job over an explicit window. It runs as a separately-named Job and
      // never advances the incremental watermark, so it does not disturb the live cursor.
      if (status.getBackfillFrom() != null && status.getBackfillTo() != null) {
        return handleBackfill(object, status);
      }

      // Find corresponding Job.
      String jobYaml = jobYaml(object);
      DynamicKubernetesObject expectedJob = yamlApi.objFromYaml(jobYaml);
      V1Job job = jobApi.getIfExists(expectedJob.getMetadata().getNamespace(), expectedJob.getMetadata().getName());

      ExecutionTime scheduled = scheduledExecution(object);
      ZonedDateTime now = ZonedDateTime.now();

      // Data-availability firing: when a source provider reports a data-time frontier for the
      // input, advance the cursor to it and launch the Job over the newly-available window
      // [watermark, timestamp]. The frontier is a completeness watermark — the source has confirmed
      // input exists through it — so no additional margin is applied. Gated on job == null (like
      // cron) so we process one window at a time. Uniform across every source: the source supplies a
      // watermark; this reconciler owns the cursor and Job launching.
      if (job == null && frontier != null
          && (status.getTimestamp() == null || frontier.isAfter(status.getTimestamp()))) {
        log.info("Advancing TableTrigger {} to input frontier {}.", name, frontier);
        status.setTimestamp(frontier);
        tableTriggerApi.updateStatus(object, status);
        return new Result(true);
      }

      // Late-change repair: when a source reports a change that landed behind the watermark (a late
      // or out-of-order write to already-processed history), replay that data-time window as a
      // one-off backfill — which never moves the cursor. Consumed in arrival order via the internal
      // lateWatermark, serialized through the single backfill slot. The user-facing watermark stays
      // the monotone forward frontier.
      if (job == null && !inputWatermarkProviders.isEmpty()
          && status.getBackfillFrom() == null && status.getWatermark() != null) {
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
   * Incremental fires pass the cursor ({@code status.watermark}/{@code status.timestamp}); backfills
   * pass their requested window. The window is exposed to the template via {@link #withInstantVars}
   * as {@code {{watermark}}}/{@code {{timestamp}}}; a job that needs a wider read range applies its
   * own policy in its SQL.
   */
  private String renderJob(V1alpha1TableTrigger trigger, OffsetDateTime watermark,
      OffsetDateTime timestamp) throws SQLException {
    Template.SimpleEnvironment env = new Template.SimpleEnvironment()
        .with("trigger", trigger.getMetadata().getName())
        .with("schema", trigger.getSpec().getSchema())
        .with("table", trigger.getSpec().getTable());
    env = withInstantVars(env, "timestamp", timestamp);
    env = withInstantVars(env, "watermark", watermark);
    Map<String, String> jobProperties = trigger.getSpec().getJobProperties();
    if (jobProperties != null) {
      Properties props = new Properties();
      props.putAll(jobProperties);
      env = env.with(props);
    }
    return new Template.SimpleTemplate(trigger.getSpec().getYaml()).render(env);
  }

  /**
   * Exports a family of template variables for one instant, so jobs can read it without parsing.
   * For base name {@code "timestamp"} and instant {@code 2026-05-08T07:55:00Z} this exports:
   *
   * <ul>
   *   <li>{@code {{timestamp}}} — ISO-8601, e.g. {@code 2026-05-08T07:55Z}.</li>
   *   <li>{@code {{timestampEpochMs}}} — Unix epoch milliseconds, e.g. {@code 1778...}.</li>
   *   <li>{@code {{timestampDate}}} — UTC calendar date, e.g. {@code 2026-05-08}.</li>
   *   <li>{@code {{timestampHour}}} — UTC hour-of-day, zero-padded, e.g. {@code 07}.</li>
   * </ul>
   *
   * <p>A null instant exports nothing (the variables are simply absent).
   */
  private static Template.SimpleEnvironment withInstantVars(Template.SimpleEnvironment env, String base,
      OffsetDateTime instant) {
    if (instant == null) {
      return env;
    }
    OffsetDateTime utc = instant.withOffsetSameInstant(ZoneOffset.UTC);
    return env
        .with(base, instant.toString())
        .with(base + "EpochMs", Long.toString(instant.toInstant().toEpochMilli()))
        .with(base + "Date", utc.toLocalDate().toString())
        .with(base + "Hour", String.format(Locale.ROOT, "%02d", utc.getHour()));
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

  /**
   * Runs a one-off backfill over {@code [status.backfillFrom, status.backfillTo]} as a separately
   * named Job ({@code <job>-bf-<windowId>}). The job name encodes the window, so a fresh backfill
   * never collides with a previous one still terminating. Unlike the incremental path, completion
   * does <em>not</em> advance the watermark — it clears the backfill request, leaving the live
   * cursor untouched. A failed backfill is abandoned (logged) and cleared, so it never starves the
   * incremental path; re-issue {@code FIRE TRIGGER ... FROM ... TO ...} to retry.
   */
  private Result handleBackfill(V1alpha1TableTrigger trigger, V1alpha1TableTriggerStatus status)
      throws SQLException {
    String yaml = renderJob(trigger, status.getBackfillFrom(), status.getBackfillTo());
    DynamicKubernetesObject expected = yamlApi.objFromYaml(yaml);
    V1ObjectMeta meta = expected.getMetadata();
    String backfillName = backfillJobName(meta.getName(), status.getBackfillFrom(), status.getBackfillTo());
    expected.setMetadata(meta.name(backfillName));

    V1Job job = jobApi.getIfExists(expected.getMetadata().getNamespace(), backfillName);
    if (job == null) {
      log.info("Launching backfill Job {} for window [{}, {}].", backfillName,
          status.getBackfillFrom(), status.getBackfillTo());
      createBackfillJob(expected, trigger);
      return new Result(true, pendingRetryDuration());
    }

    if (job.getStatus() == null || job.getStatus().getConditions() == null) {
      log.info("Backfill Job {} has no status yet.", backfillName);
      return new Result(true, pendingRetryDuration());
    }
    List<V1JobCondition> conditions = job.getStatus().getConditions();
    boolean failed = conditions.stream()
        .anyMatch(x -> "Failed".equals(x.getType()) && "True".equals(x.getStatus()));
    boolean complete = conditions.stream()
        .anyMatch(x -> "Complete".equals(x.getType()) && "True".equals(x.getStatus()));

    // Clear the request BEFORE deleting the Job. If the clear fails (e.g. a 409), we never deleted
    // the Job, so the next reconcile re-observes the same terminal Job and retries the clear — the
    // backfill is never re-run. (Delete-then-clear could lose the clear and re-launch the job.)
    if (failed) {
      log.error("Backfill Job {} FAILED; abandoning backfill [{}, {}]. Re-issue FIRE to retry.",
          backfillName, status.getBackfillFrom(), status.getBackfillTo());
      clearBackfill(trigger, status);
      jobApi.delete(job);
      return new Result(true);
    } else if (complete) {
      log.info("Backfill Job {} completed; watermark left untouched.", backfillName);
      clearBackfill(trigger, status);
      jobApi.delete(job);
      return new Result(true);
    } else {
      log.info("Backfill Job {} still running.", backfillName);
      return new Result(true, pendingRetryDuration());
    }
  }

  /**
   * Deterministic name for a backfill Job: {@code <base>-bf-<hex windowId>}. The id is derived from
   * the window bounds, so distinct windows get distinct Job names and re-firing the same window is
   * idempotent. Stable across JVMs ({@link String#hashCode} is specified).
   */
  static String backfillJobName(String base, OffsetDateTime from, OffsetDateTime to) {
    String windowId = Integer.toHexString((from.toString() + "/" + to.toString()).hashCode());
    return base + BACKFILL_INFIX + windowId;
  }

  private void createBackfillJob(DynamicKubernetesObject job, V1alpha1TableTrigger trigger)
      throws SQLException {
    Map<String, String> annotations = new HashMap<>();
    annotations.put(TRIGGER_KEY, trigger.getMetadata().getName());
    annotations.put(BACKFILL_KEY, "true");
    Map<String, String> labels = new HashMap<>();
    labels.put(TRIGGER_KEY, trigger.getMetadata().getName());
    labels.put(BACKFILL_KEY, "true");
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
    yamlApi.createWithMetadata(job, annotations, labels, ownerReference);
  }

  private void clearBackfill(V1alpha1TableTrigger trigger, V1alpha1TableTriggerStatus status)
      throws SQLException {
    status.setBackfillFrom(null);
    status.setBackfillTo(null);
    tableTriggerApi.updateStatus(trigger, status);
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
   * Resolves the input's data-time completeness watermark from the registered
   * {@link InputWatermarkProvider}s, in UTC, or null when no provider handles the input (the
   * trigger is then cron-/manually-driven). See {@link InputWatermarkService}.
   */
  private OffsetDateTime inputFrontier(V1alpha1TableTrigger trigger) {
    if (inputWatermarkProviders.isEmpty()) {
      return null;
    }
    V1alpha1TableTriggerSpec spec = trigger.getSpec();
    TriggerInput input = new TriggerInput(spec.getCatalog(), spec.getSchema(), spec.getTable());
    Optional<Instant> watermark = InputWatermarkService.completeThrough(inputWatermarkProviders, input);
    return watermark.map(t -> t.atOffset(ZoneOffset.UTC)).orElse(null);
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
    TriggerInput input = new TriggerInput(spec.getCatalog(), spec.getSchema(), spec.getTable());
    OffsetDateTime watermark = status.getWatermark();

    if (status.getLateWatermark() == null) {
      status.setLateWatermark(OffsetDateTime.now(ZoneOffset.UTC));
      tableTriggerApi.updateStatus(object, status);
      return new Result(true);
    }
    Instant since = status.getLateWatermark().toInstant();

    List<DataChange> changes = new ArrayList<>();
    for (InputWatermarkProvider provider : inputWatermarkProviders) {
      try {
        changes.addAll(provider.changesSince(input, since));
      } catch (Exception e) {
        log.warn("InputWatermarkProvider {} changesSince failed for {}; skipping.",
            provider.getClass().getName(), input, e);
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
        status.setBackfillFrom(windowStart);
        status.setBackfillTo(cappedEnd);
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

