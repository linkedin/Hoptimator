package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.util.Template;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;


/**
 * Renders and creates the Jobs a {@code TableTrigger} launches, shared by the reconciler (incremental
 * fires) and {@link K8sTriggerDeployer} ({@code FIRE ... FROM ... TO ...}).
 *
 * <p><b>Backfills are one-off Jobs, not tracked state.</b> A backfill runs a job over an explicit
 * data-time window and never touches the trigger's cursor, so it is exactly a one-off Kubernetes
 * Job: {@link #createBackfill} renders the window and creates a separately-named, trigger-owned Job,
 * then forgets it. The Kubernetes Job controller owns its lifecycle — retry via the template's
 * {@code backoffLimit}, cleanup via {@code ttlSecondsAfterFinished}, and a failed Job is its own
 * durable, inspectable record (labelled {@code backfill=true}). Nothing is written to the trigger's
 * status, so there is no slot to serialize on and no request to lose.
 */
public final class K8sTriggerJobs {

  static final String TRIGGER_KEY = "trigger";
  static final String BACKFILL_KEY = "backfill";
  static final String BACKFILL_INFIX = "-bf-";
  /** Kubernetes object names are capped at 63 characters. */
  static final int MAX_JOB_NAME = 63;

  /**
   * The template variables that name the fire window, resolved per-fire by {@link #render} (and its
   * {@link #withInstantVars} derived forms). A JobTemplate reads these to know which data-time window
   * the launched Job must cover. Kept here as the single source of truth so the CREATE-TRIGGER render
   * can defer exactly these (see {@link #deferredWindowVars}).
   */
  static final List<String> WINDOW_VARS = Collections.unmodifiableList(Arrays.asList(
      "watermark", "watermarkEpochMs", "watermarkDate", "watermarkHour",
      "timestamp", "timestampEpochMs", "timestampDate", "timestampHour"));

  private K8sTriggerJobs() {
  }

  /**
   * Whether {@code key} names a fire-window variable — one resolved per-fire by {@link #render}
   * (and its {@link #withInstantVars} derived forms), not at CREATE time. A JobTemplate rendered by
   * {@code CREATE TRIGGER ... as <template>} is rendered twice: once to fix the static vars and again
   * per-fire to fill the window. Passing this as the {@code defer} predicate to
   * {@link Template.SimpleTemplate#render(Template.Environment, java.util.function.Predicate)} leaves
   * the window tokens (transform and all) intact for that second render, while any other unresolved
   * variable still fails the first render — so CREATE TRIGGER rejects a genuinely missing variable
   * rather than deferring the failure to fire time.
   */
  public static boolean isWindowVar(String key) {
    return WINDOW_VARS.contains(key);
  }

  /**
   * Renders the trigger's Job template for an explicit output window {@code [windowStart,
   * windowEnd]}. The window is exposed to the template as {@code {{watermark}}}/{@code {{timestamp}}}
   * (plus the convenience forms from {@link #withInstantVars}); a job that needs a wider read range
   * applies its own policy in its SQL. A null bound (e.g. the first fire has no prior watermark) is
   * exposed as an empty string, so a template that references the window still renders.
   *
   * @throws SQLException if the template renders to nothing — a referenced {@code {{variable}}} the
   *     per-fire environment does not provide. This surfaces the failure with the trigger's name
   *     instead of letting a null propagate into an opaque NPE downstream.
   */
  public static String render(V1alpha1TableTrigger trigger, OffsetDateTime windowStart,
      OffsetDateTime windowEnd) throws SQLException {
    Template.SimpleEnvironment env = new Template.SimpleEnvironment()
        .with("trigger", trigger.getMetadata().getName())
        .with("schema", trigger.getSpec().getSchema())
        .with("table", trigger.getSpec().getTable());
    env = withInstantVars(env, "timestamp", windowEnd);
    env = withInstantVars(env, "watermark", windowStart);
    Map<String, String> jobProperties = trigger.getSpec().getJobProperties();
    if (jobProperties != null) {
      Properties props = new Properties();
      props.putAll(jobProperties);
      env = env.with(props);
    }
    String rendered = new Template.SimpleTemplate(trigger.getSpec().getYaml()).render(env);
    if (rendered == null || rendered.trim().isEmpty()) {
      throw new SQLException("Trigger " + trigger.getMetadata().getName() + ": its Job template "
          + "rendered to nothing. It references a template variable that could not be resolved at "
          + "fire time — the per-fire environment provides trigger/schema/table, the fire window "
          + "(watermark/timestamp and their derived forms), and job.properties.*, but nothing else. "
          + "Check the operator log for \"resolved to null. Skipping template.\".");
    }
    return rendered;
  }

  /**
   * Exports a family of template variables for one instant, so jobs can read it without parsing:
   * {@code {{base}}} (ISO-8601), {@code {{base}}EpochMs}, {@code {{base}}Date} (UTC date), and
   * {@code {{base}}Hour} (UTC hour). A null instant (e.g. the first fire has no prior watermark)
   * exports the family as empty strings, so a template referencing the window still renders and the
   * job decides how to treat the open bound.
   */
  static Template.SimpleEnvironment withInstantVars(Template.SimpleEnvironment env, String base,
      OffsetDateTime instant) {
    if (instant == null) {
      return env
          .with(base, "")
          .with(base + "EpochMs", "")
          .with(base + "Date", "")
          .with(base + "Hour", "");
    }
    OffsetDateTime utc = instant.withOffsetSameInstant(ZoneOffset.UTC);
    return env
        .with(base, instant.toString())
        .with(base + "EpochMs", Long.toString(instant.toInstant().toEpochMilli()))
        .with(base + "Date", utc.toLocalDate().toString())
        .with(base + "Hour", String.format(Locale.ROOT, "%02d", utc.getHour()));
  }

  /**
   * Deterministic name for a backfill Job: {@code <base>-bf-<hex windowId>}. The id is derived from
   * the window bounds (and {@code discriminator}, when present), so distinct windows get distinct Job
   * names. Re-firing the same window is idempotent (same name); auto-repair passes the change's
   * arrival as the discriminator so genuinely new late data on the same window gets a fresh Job.
   * Stable across JVMs ({@link String#hashCode} is specified).
   *
   * <p>Kubernetes caps object names at 63 characters. When {@code base} is long enough that the full
   * name would overflow (common for real trigger names), the base is shortened to a readable
   * prefix plus a hash of the full base — keeping the name deterministic, unique, and valid.
   */
  public static String backfillJobName(String base, OffsetDateTime from, OffsetDateTime to,
      OffsetDateTime discriminator) {
    String key = from.toString() + "/" + to.toString() + (discriminator == null ? "" : "/" + discriminator);
    String suffix = BACKFILL_INFIX + Integer.toHexString(key.hashCode());
    if (base.length() + suffix.length() <= MAX_JOB_NAME) {
      return base + suffix;
    }
    // Too long for K8s: keep a readable prefix and append a hash of the full base for uniqueness.
    String baseHash = Integer.toHexString(base.hashCode());
    int keep = MAX_JOB_NAME - suffix.length() - baseHash.length() - 1;   // -1 for the joining '-'
    String prefix = base.substring(0, Math.max(0, keep));
    while (prefix.endsWith("-")) {
      prefix = prefix.substring(0, prefix.length() - 1);
    }
    return prefix + "-" + baseHash + suffix;
  }

  /**
   * Renders {@code [from, to]} and creates a one-off backfill Job, owned by the trigger and labelled
   * {@code backfill=true}, then forgets it — the Kubernetes Job controller owns its lifecycle. The
   * create is idempotent by the deterministic Job name, so a retried reconcile (or a re-issued FIRE)
   * over the same window and {@code discriminator} does not launch a duplicate.
   */
  public static void createBackfill(K8sYamlApi yamlApi, V1alpha1TableTrigger trigger,
      OffsetDateTime from, OffsetDateTime to, OffsetDateTime discriminator) throws SQLException {
    DynamicKubernetesObject job = yamlApi.objFromYaml(render(trigger, from, to));
    V1ObjectMeta meta = job.getMetadata();
    String name = backfillJobName(meta.getName(), from, to, discriminator);
    job.setMetadata(meta.name(name));

    if (yamlApi.getIfExists(job) != null) {
      return;  // idempotent: the Job for this window already exists.
    }

    Map<String, String> metadata = new HashMap<>();
    metadata.put(TRIGGER_KEY, trigger.getMetadata().getName());
    metadata.put(BACKFILL_KEY, "true");
    yamlApi.createWithMetadata(job, metadata, metadata, ownerReferences(trigger));
  }

  private static List<V1OwnerReference> ownerReferences(V1alpha1TableTrigger trigger) {
    if (trigger.getMetadata().getOwnerReferences() != null
        && !trigger.getMetadata().getOwnerReferences().isEmpty()) {
      return trigger.getMetadata().getOwnerReferences();
    }
    return Collections.singletonList(new V1OwnerReference()
        .apiVersion(trigger.getApiVersion())
        .kind(trigger.getKind())
        .name(trigger.getMetadata().getName())
        .uid(trigger.getMetadata().getUid()));
  }
}
