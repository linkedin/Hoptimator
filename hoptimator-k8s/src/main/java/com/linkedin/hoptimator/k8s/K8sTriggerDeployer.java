package com.linkedin.hoptimator.k8s;

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


class K8sTriggerDeployer extends K8sDeployer<V1alpha1TableTrigger, V1alpha1TableTriggerList> {

  private final K8sContext context;
  private final Trigger trigger;
  private final K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi;
  private final K8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> jobTemplateApi;

  K8sTriggerDeployer(Trigger trigger, K8sContext context) {
    super(context, K8sApiEndpoints.TABLE_TRIGGERS);
    this.context = context;
    this.trigger = trigger;
    this.triggerApi = new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS);
    this.jobTemplateApi = new K8sApi<>(context, K8sApiEndpoints.JOB_TEMPLATES);
  }

  @Override
  public void update() throws SQLException {
    if (trigger.options().containsKey(Trigger.PAUSED_OPTION)) {
      String pauseValue = trigger.options().get(Trigger.PAUSED_OPTION);
      String canonicalName = K8sUtils.canonicalizeName(trigger.name());
      V1alpha1TableTrigger existingTrigger = triggerApi.get(canonicalName);

      if (existingTrigger == null) {
        throw new SQLException("Trigger " + trigger.name() + " not found.");
      }

      V1alpha1TableTriggerSpec spec = existingTrigger.getSpec();
      if (spec == null) {
        spec = new V1alpha1TableTriggerSpec();
        existingTrigger.spec(spec);
      }
      spec.setPaused("true".equals(pauseValue));
      triggerApi.update(existingTrigger);
      return;
    }
    super.update();
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
    String name = K8sUtils.canonicalizeName(trigger.name(), trigger.job().name());
    String triggerName = K8sUtils.canonicalizeName(trigger.name());
    String viewName = K8sUtils.canonicalizeName(trigger.path());
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
        .with("table", trigger.table())
        .with("schema", trigger.schema())
        .with(properties);
    V1alpha1JobTemplate jobTemplate = jobTemplateApi.get(jobNamespace, jobName);
    Map<String, String> labels = new HashMap<String, String>();
    labels.put("view", viewName); // a corresponding view object may or may not exist.
    String template = jobTemplate.getSpec().getYaml();
    String rendered = new Template.SimpleTemplate(template).render(env);
    return new V1alpha1TableTrigger()
        .kind(K8sApiEndpoints.TABLE_TRIGGERS.kind())
        .apiVersion(K8sApiEndpoints.TABLE_TRIGGERS.apiVersion())
        .metadata(new V1ObjectMeta().name(triggerName).labels(labels))
        .spec(new V1alpha1TableTriggerSpec()
        .schema(trigger.schema())
        .table(trigger.table())
        .schedule(trigger.cronSchedule())
        .yaml(rendered));
  }
}
