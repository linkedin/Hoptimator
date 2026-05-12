package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Trigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.util.Template;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class K8sTriggerDeployer extends K8sDeployer<V1alpha1TableTrigger, V1alpha1TableTriggerList> {

  /** Annotation carrying the JobTemplate name (e.g. {@code retl-job-template}) used to render this trigger. */
  static final String JOB_TEMPLATE_ANNOTATION = "hoptimator.linkedin.com/job-template";

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
      // or missing depends-on metadata, breaking the visualizer's reverse lookup.
      stampDependencyMetadata(existingTrigger);
      triggerApi.update(existingTrigger);
      return;
    }
    super.update();
  }

  private void stampDependencyMetadata(V1alpha1TableTrigger target) {
    V1ObjectMeta meta = target.getMetadata();
    if (meta == null) {
      meta = new V1ObjectMeta();
      target.metadata(meta);
    }
    Map<String, String> labels = meta.getLabels() != null ? meta.getLabels() : new HashMap<>();
    Map<String, String> annotations = meta.getAnnotations() != null ? meta.getAnnotations() : new HashMap<>();
    Source source = trigger.source();
    if (source != null && source.database() != null) {
      String labelKey = PipelineDependencyLabels.labelKey(source.database(), source.path());
      String identifier = PipelineDependencyLabels.identifier(source.database(), source.path());
      labels.put(labelKey, identifier.length() <= 63 ? identifier : identifier.substring(0, 63));
      annotations.put(PipelineDependencyLabels.ANNOTATION_KEY_SOURCES, identifier);
    }
    Sink sink = trigger.sink();
    if (sink != null) {
      String sinkLabelKey = PipelineDependencyLabels.labelKey(sink.database(), sink.path());
      String sinkIdentifier = PipelineDependencyLabels.identifier(sink.database(), sink.path());
      labels.put(sinkLabelKey,
          sinkIdentifier.length() <= 63 ? sinkIdentifier : sinkIdentifier.substring(0, 63));
      annotations.put(PipelineDependencyLabels.ANNOTATION_KEY_SINK, sinkIdentifier);
    }
    if (trigger.job() != null) {
      annotations.put(JOB_TEMPLATE_ANNOTATION, trigger.job().name());
    }
    meta.setLabels(labels);
    meta.setAnnotations(annotations);
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
        .with("table", source != null ? source.table() : null)
        .with("schema", source != null ? source.schema() : null)
        .with(properties);
    V1alpha1JobTemplate jobTemplate = jobTemplateApi.get(jobNamespace, jobName);
    Map<String, String> labels = new HashMap<>();
    labels.put("view", viewName); // a corresponding view object may or may not exist.
    Map<String, String> annotations = new HashMap<>();
    // Stamp depends-on labels so the visualizer (and the dep-guard) can find triggers via
    // label selector. The Trigger's source is the upstream table the job reads from. When the
    // trigger carries a Sink (set by LogicalTableDeployer for bridging triggers), we
    // additionally stamp the sink — that's what makes reverse-ETL flows render as connectors.
    if (source != null && source.database() != null) {
      String labelKey = PipelineDependencyLabels.labelKey(source.database(), source.path());
      String identifier = PipelineDependencyLabels.identifier(source.database(), source.path());
      labels.put(labelKey, identifier.length() <= 63 ? identifier : identifier.substring(0, 63));
      annotations.put(PipelineDependencyLabels.ANNOTATION_KEY_SOURCES, identifier);
    }
    Sink sink = trigger.sink();
    if (sink != null) {
      String sinkLabelKey = PipelineDependencyLabels.labelKey(sink.database(), sink.path());
      String sinkIdentifier = PipelineDependencyLabels.identifier(sink.database(), sink.path());
      labels.put(sinkLabelKey,
          sinkIdentifier.length() <= 63 ? sinkIdentifier : sinkIdentifier.substring(0, 63));
      annotations.put(PipelineDependencyLabels.ANNOTATION_KEY_SINK, sinkIdentifier);
    }
    annotations.put(JOB_TEMPLATE_ANNOTATION, trigger.job().name());
    String template = jobTemplate.getSpec().getYaml();
    String rendered = new Template.SimpleTemplate(template).render(env);
    Map<String, String> jobProps = new HashMap<>();
    trigger.options().forEach((key, value) -> {
      if (key.startsWith("job.properties.")) {
        jobProps.put(key.substring("job.properties.".length()), value);
      }
    });
    V1alpha1TableTriggerSpec spec = new V1alpha1TableTriggerSpec()
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
    V1ObjectMeta meta = new V1ObjectMeta().name(triggerName).labels(labels);
    if (annotations != null) {
      meta.setAnnotations(annotations);
    }
    return new V1alpha1TableTrigger()
        .kind(K8sApiEndpoints.TABLE_TRIGGERS.kind())
        .apiVersion(K8sApiEndpoints.TABLE_TRIGGERS.apiVersion())
        .metadata(meta)
        .spec(spec);
  }
}
