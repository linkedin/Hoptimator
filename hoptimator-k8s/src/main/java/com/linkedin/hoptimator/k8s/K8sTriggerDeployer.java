package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Trigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import com.linkedin.hoptimator.util.Template;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class K8sTriggerDeployer extends K8sDeployer<V1alpha1TableTrigger, V1alpha1TableTriggerList> {

  private static final Logger logger = LoggerFactory.getLogger(K8sTriggerDeployer.class);

  private final K8sContext context;
  private final Trigger trigger;
  private final K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi;
  private final K8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> jobTemplateApi;

  public K8sTriggerDeployer(Trigger trigger, K8sContext context) {
    super(context, K8sApiEndpoints.TABLE_TRIGGERS);
    this.context = context;
    this.trigger = trigger;
    this.triggerApi = createTriggerApi(context);
    this.jobTemplateApi = createJobTemplateApi(context);
  }

  K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> createTriggerApi(K8sContext context) {
    return new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS);
  }

  K8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> createJobTemplateApi(K8sContext context) {
    return new K8sApi<>(context, K8sApiEndpoints.JOB_TEMPLATES);
  }

  @Override
  public void update() throws SQLException {
    String canonicalName = K8sUtils.canonicalizeName(trigger.name());
    V1alpha1TableTrigger existingTrigger = triggerApi.getIfExists(context.namespace(), canonicalName);

    if (trigger.options().containsKey(Trigger.FIRE_OPTION)) {
      fire(existingTrigger);
      return;
    }

    Boolean targetPaused = null;
    if (trigger.options().containsKey(Trigger.PAUSED_OPTION)) {
      targetPaused = Boolean.TRUE.toString().equals(trigger.options().get(Trigger.PAUSED_OPTION));
    } else if (existingTrigger != null && existingTrigger.getSpec() != null) {
      targetPaused = existingTrigger.getSpec().getPaused();
    }

    if (targetPaused != null) {
      if (existingTrigger == null) {
        throw new SQLException("Trigger " + trigger.name() + " not found.");
      }
      V1alpha1TableTriggerSpec spec = existingTrigger.getSpec();
      if (spec == null) {
        spec = new V1alpha1TableTriggerSpec();
        existingTrigger.spec(spec);
      }
      spec.setPaused(targetPaused);
      // Refresh dependency-tracking labels and annotation here too — without this, the partial
      // update path (used when the LogicalTable is re-applied) would leave triggers with stale
      // or missing depends-on metadata.
      stampDependencyMetadata(existingTrigger);
      triggerApi.update(existingTrigger);
      return;
    }
    super.update();
  }

  /** Applies a FIRE intent: rejects on in-flight (incremental or backfill), validates any requested
   *  backfill window, then writes status only (never the spec) — a single update. A plain fire bumps
   *  status.timestamp; a windowed fire records a one-off backfill via status.backfillFrom/backfillTo,
   *  leaving the watermark untouched. The TableTriggerReconciler materialises the Job. */
  private void fire(V1alpha1TableTrigger existingTrigger) throws SQLException {
    if (existingTrigger == null) {
      throw new SQLException("Trigger " + trigger.name() + " not found.");
    }
    V1alpha1TableTriggerStatus status = existingTrigger.getStatus();
    if (status != null && status.getTimestamp() != null
        && (status.getWatermark() == null || status.getTimestamp().isAfter(status.getWatermark()))) {
      throw new SQLException("Trigger " + trigger.name() + " has an in-flight execution (timestamp="
          + status.getTimestamp() + ", watermark=" + status.getWatermark()
          + "). Wait for it to complete, or pause/resume to abort.");
    }
    if (status != null && status.getBackfillFrom() != null) {
      throw new SQLException("Trigger " + trigger.name() + " already has a backfill in flight ["
          + status.getBackfillFrom() + ", " + status.getBackfillTo() + "]. Wait for it to complete.");
    }

    // Validate a requested backfill window up front, before mutating anything. A backfill may only
    // cover already-processed history, so the end is capped at the watermark — a backfill can never
    // run ahead of the incremental cursor. We reject only a window that is inverted or lies entirely
    // at/after the watermark (nothing to backfill).
    OffsetDateTime backfillFrom = null;
    OffsetDateTime backfillTo = null;
    String fireFromOpt = trigger.options().get(Trigger.FIRE_FROM_OPTION);
    String fireToOpt = trigger.options().get(Trigger.FIRE_TO_OPTION);
    if (fireFromOpt != null && fireToOpt != null) {
      backfillFrom = OffsetDateTime.parse(fireFromOpt);
      backfillTo = OffsetDateTime.parse(fireToOpt);
      if (!backfillFrom.isBefore(backfillTo)) {
        throw new SQLException("Backfill window start (" + backfillFrom + ") must be before end ("
            + backfillTo + ").");
      }
      OffsetDateTime watermark = status != null ? status.getWatermark() : null;
      if (watermark == null) {
        throw new SQLException("Cannot backfill trigger " + trigger.name()
            + ": it has no watermark yet, so there is no processed history to backfill.");
      }
      if (!backfillFrom.isBefore(watermark)) {
        throw new SQLException("Backfill window [" + backfillFrom + ", " + backfillTo
            + "] is entirely at or after the watermark (" + watermark + "); there is no processed "
            + "history to backfill. The incremental cursor will reach this range on its own.");
      }
      if (backfillTo.isAfter(watermark)) {
        logger.info("Capping backfill end for trigger {} from {} to the watermark {} "
            + "(a backfill cannot cover data the cursor has not yet processed).",
            trigger.name(), backfillTo, watermark);
        backfillTo = watermark;
      }
    }

    // FIRE never mutates the spec — it only updates status (a single write). This is the gating
    // commit: if it fails (e.g. a 409 conflict), the FIRE surfaces an error and the client retries.
    V1alpha1TableTriggerStatus newStatus = status != null ? status : new V1alpha1TableTriggerStatus();
    if (backfillFrom != null && backfillTo != null) {
      // Windowed fire: request a one-off backfill over [from, to]. The reconciler runs it as a
      // separate Job and does NOT advance the cursor — watermark/timestamp are left untouched.
      newStatus.setBackfillFrom(backfillFrom);
      newStatus.setBackfillTo(backfillTo);
    } else {
      newStatus.setTimestamp(OffsetDateTime.now(ZoneOffset.UTC));
    }
    existingTrigger.setStatus(newStatus);
    triggerApi.updateStatus(existingTrigger, newStatus);
  }

  private void stampDependencyMetadata(V1alpha1TableTrigger target) {
    V1ObjectMeta meta = target.getMetadata();
    if (meta == null) {
      meta = new V1ObjectMeta();
      target.metadata(meta);
    }
    Source source = trigger.source();
    DependencyLabels.stamp(meta,
        source != null ? Collections.singletonList(source) : Collections.emptyList(),
        trigger.sink() != null ? Collections.singletonList(trigger.sink()) : Collections.emptyList());
  }

  @Override
  public void delete() throws SQLException {
    String canonicalName = K8sUtils.canonicalizeName(trigger.name());
    V1alpha1TableTrigger existingTrigger = triggerApi.get(canonicalName);
    if (existingTrigger == null) {
      throw new SQLException("Trigger " + trigger.name() + " not found.");
    }
    triggerApi.delete(existingTrigger);
  }

  @Override
  protected V1alpha1TableTrigger toK8sObject() throws SQLException {
    Source source = trigger.source();
    String name = K8sUtils.canonicalizeName(trigger.name(), trigger.job().name());
    String triggerName = K8sUtils.canonicalizeName(trigger.name());
    String viewName = source != null ? K8sUtils.canonicalizeName(source.path()) : triggerName;
    String jobName = K8sUtils.canonicalizeName(trigger.job().name());
    String jobNamespace = trigger.job().namespace() != null ? trigger.job().namespace()
        : context.namespace();
    Properties properties = new Properties();
    properties.putAll(trigger.options());
    Template.Environment env = new Template.SimpleEnvironment()
        .with("name", name)
        .with("trigger", triggerName)
        .with("job", jobName)
        .with("schedule", trigger.cronSchedule())
        .with("path", source != null ? source.pathString() : null)
        .with("table", source != null ? source.table() : null)
        .with("schema", source != null ? source.schema() : null)
        .with("catalog", source != null ? source.catalog() : null)
        .with(properties);
    V1alpha1JobTemplate jobTemplate = jobTemplateApi.get(jobNamespace, jobName);
    V1ObjectMeta meta = new V1ObjectMeta().name(triggerName);
    Map<String, String> labels = new HashMap<>();
    labels.put("view", viewName); // a corresponding view object may or may not exist.
    meta.setLabels(labels);
    // Stamp depends-on labels so the dep-guard can find triggers via
    // label selector. The Trigger's source is the upstream table the job reads from. When the
    // trigger carries a Sink, we additionally stamp the sink.
    DependencyLabels.stamp(meta,
        source != null ? Collections.singletonList(source) : Collections.emptyList(),
        trigger.sink() != null ? Collections.singletonList(trigger.sink()) : Collections.emptyList());
    String template = jobTemplate.getSpec().getYaml();
    String rendered = new Template.SimpleTemplate(template).render(env);
    Map<String, String> jobProps = new HashMap<>();
    trigger.options().forEach((key, value) -> {
      if (key.startsWith("job.properties.")) {
        jobProps.put(key.substring("job.properties.".length()), value);
      }
    });
    V1alpha1TableTriggerSpec spec = new V1alpha1TableTriggerSpec()
        .catalog(source != null ? source.catalog() : null)
        .schema(source != null ? source.schema() : null)
        .table(source != null ? source.table() : null)
        .schedule(trigger.cronSchedule())
        .yaml(rendered);
    if (!jobProps.isEmpty()) {
      spec.jobProperties(jobProps);
    }
    if (trigger.options().containsKey(Trigger.PAUSED_OPTION)) {
      spec.paused("true".equals(trigger.options().get(Trigger.PAUSED_OPTION)));
    }
    return new V1alpha1TableTrigger()
        .kind(K8sApiEndpoints.TABLE_TRIGGERS.kind())
        .apiVersion(K8sApiEndpoints.TABLE_TRIGGERS.apiVersion())
        .metadata(meta)
        .spec(spec);
  }
}
