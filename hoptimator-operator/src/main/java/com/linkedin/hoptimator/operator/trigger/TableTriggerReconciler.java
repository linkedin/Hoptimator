package com.linkedin.hoptimator.operator.trigger;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.time.OffsetDateTime;

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
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sYamlApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sUtils;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import com.linkedin.hoptimator.util.Template;


/**
 * Launches Jobs when TableTriggers are fired.
 */
public final class TableTriggerReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(TableTriggerReconciler.class);
  private final static String TRIGGER_KEY = "TRIGGER";
  private final static String TRIGGER_TIMESTAMP_KEY = "TRIGGER_TIMESTAMP";

  private final K8sContext context;
  private final K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi;
  private final K8sApi<V1Job, V1JobList> jobApi;
  private final K8sYamlApi yamlApi;

  private TableTriggerReconciler(K8sContext context) {
    this.context = context;
    this.tableTriggerApi = new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS);
    this.jobApi = new K8sApi<>(context, K8sApiEndpoints.JOBS);
    this.yamlApi = new K8sYamlApi(context);
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
        log.info("Trigger has not been fired yet. Skipping.", name);
        return new Result(false);
      }

      log.info("TableTrigger {} was last fired at {}.", name, status.getTimestamp());

      if (object.getSpec().getYaml() == null) {
        log.info("Trigger has no Job YAML. Will take no action.", name);
        return new Result(false);
      }

      // Find corresponding Job.
      Collection<V1Job> jobs = jobApi.select(TRIGGER_KEY + " = " + name);
      V1Job job = jobs.stream().findFirst().orElse(null);  // assume only one job.

      boolean shouldFire = true;
      if (object.getStatus() != null && object.getStatus().getTimestamp() != null
          && object.getStatus().getWatermark() != null
          && !object.getStatus().getTimestamp().isAfter(object.getStatus().getWatermark())) {
        shouldFire = false;
      }

      if (job == null && shouldFire) {
        log.info("Launching Job for TableTrigger {}. ", name);
        createJob(object);
        return new Result(true, pendingRetryDuration());
      } else if (job != null && job.getStatus() != null && job.getStatus().getConditions() != null) {
        List<V1JobCondition> conditions = job.getStatus().getConditions();
        boolean failed = conditions.stream().anyMatch(x -> x.getType().equals("Failed"));
        boolean complete = conditions.stream().anyMatch(x -> x.getType().equals("Complete"));
        if (failed) {
          log.warn("Job {} has FAILED.", name);
        } else if (complete) {
          log.info("Job {} completed successfully.", name);
          // We get the watermark from the job itself. We annotate the job when launching it.
          if (job.getMetadata().getAnnotations() == null || job.getMetadata().getAnnotations()
              .get(TRIGGER_TIMESTAMP_KEY) == null) {
            log.error("Job {} has no timestamp annotation. Unable to progress the watermark.", name);
          } else if (object.getStatus().getWatermark() == null) {
            log.info("Trigger {} has no existing watermark.", name);
            String watermark = job.getMetadata().getAnnotations().get(TRIGGER_TIMESTAMP_KEY);
            status.setWatermark(OffsetDateTime.parse(watermark));
            tableTriggerApi.updateStatus(object, status);
            log.info("Trigger {} watermark set to {}.", name, watermark);
          } else {
            OffsetDateTime timestamp = OffsetDateTime.parse(job.getMetadata().getAnnotations().get(TRIGGER_TIMESTAMP_KEY));
            status.setWatermark(timestamp);
            tableTriggerApi.updateStatus(object, status);
            log.info("Trigger {} watermark advanced to {}.", name, timestamp);
          }
        }
        if (failed || complete) {
          jobApi.delete(job);
          return new Result(true);  // start over
        } else {
          log.info("Job for TableTrigger {} still running from a previous trigger event.", name);
          return new Result(true, pendingRetryDuration());  // retry later
        }
      } else {
        log.info("Job for TableTrigger {} has no status yet.", name);
        return new Result(true, pendingRetryDuration());  // retry later
      }
    } catch (Exception e) {
      log.error("Encountered exception while reconciling TableTrigger {}.", name, e);
      return new Result(true, failureRetryDuration());
    }
  }

  private void createJob(V1alpha1TableTrigger trigger) throws SQLException {
    V1alpha1TableTriggerStatus status = trigger.getStatus();
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

