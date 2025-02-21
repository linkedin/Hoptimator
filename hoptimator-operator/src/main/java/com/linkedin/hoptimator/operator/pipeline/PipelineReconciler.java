package com.linkedin.hoptimator.operator.pipeline;

import java.time.Duration;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sUtils;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineStatus;


/**
 * Manages Pipelines.
 */
public final class PipelineReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(PipelineReconciler.class);

  private final K8sContext context;
  private final K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi;

  private PipelineReconciler(K8sContext context) {
    this.context = context;
    this.pipelineApi = new K8sApi<>(context, K8sApiEndpoints.PIPELINES);
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    Result result = new Result(true, pendingRetryDuration());
    try {
      V1alpha1Pipeline object = pipelineApi.get(namespace, name);

      if (object == null) {
        log.info("Object {} deleted. Skipping.", name);
        return new Result(false);
      }

      V1alpha1PipelineStatus status = object.getStatus();
      if (status == null) {
        status = new V1alpha1PipelineStatus();
        object.setStatus(status);
      }

      log.info("Checking status of Pipeline {}...", name);

      boolean ready = Arrays.asList(object.getSpec().getYaml().split("\n---\n"))
          .stream()
          .filter(x -> x != null && !x.isEmpty())
          .allMatch(x -> isReady(x));

      if (ready) {
        status.setReady(true);
        status.setFailed(false);
        status.setMessage("Ready.");
        log.info("Pipeline {} is ready.", name);
        result = new Result(false);
      } else {
        status.setReady(false);
        status.setFailed(false);
        status.setMessage("Deployed.");
        log.info("Pipeline {} is NOT ready.", name);
      }

      pipelineApi.updateStatus(object, status);
    } catch (Exception e) {
      log.error("Encountered exception while reconciling Pipeline {}.", name, e);
      return new Result(true, failureRetryDuration());
    }
    return result;
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
    Reconciler reconciler = new PipelineReconciler(context);
    return ControllerBuilder.defaultBuilder(context.informerFactory())
        .withReconciler(reconciler)
        .withName("pipeline-controller")
        .withWorkerCount(1)
        .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1Pipeline.class, x).build())
        .build();
  }

  private boolean isReady(String yaml) {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    String namespace = obj.getMetadata().getNamespace();
    String name = obj.getMetadata().getName();
    String kind = obj.getKind();
    try {
      KubernetesApiResponse<DynamicKubernetesObject> existing =
          context.dynamic(obj.getApiVersion(), K8sUtils.guessPlural(obj)).get(namespace, name);
      existing.onFailure((code, status) -> log.warn("Failed to fetch {}/{}: {}.", kind, name, status.getMessage()));
      if (!existing.isSuccess()) {
        return false;
      }
      if (guessReady(existing.getObject())) {
        log.info("{}/{} is ready.", kind, name);
        return true;
      } else {
        log.info("{}/{} is NOT ready.", kind, name);
        return false;
      }
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean guessReady(DynamicKubernetesObject obj) {
    // We make a best effort to guess the status of the dynamic object. By default, it's ready.
    if (obj == null || obj.getRaw() == null) {
      return false;
    }
    try {
      return obj.getRaw().get("status").getAsJsonObject().get("ready").getAsBoolean();
    } catch (Exception e) {
      log.debug("Exception looking for .status.ready. Swallowing.", e);
    }
    try {
      return obj.getRaw()
          .get("status")
          .getAsJsonObject()
          .get("state")
          .getAsString()
          .matches("(?i)READY|RUNNING|FINISHED");
    } catch (Exception e) {
      log.debug("Exception looking for .status.state. Swallowing.", e);
    }
    try {
      return obj.getRaw()
          .get("status")
          .getAsJsonObject()
          .get("jobStatus")
          .getAsJsonObject()
          .get("state")
          .getAsString()
          .matches("(?i)READY|RUNNING|FINISHED");
    } catch (Exception e) {
      log.debug("Exception looking for .status.jobStatus.state. Swallowing.", e);
    }
    // TODO: Look for common Conditions
    log.warn("Object {}/{}/{} considered ready by default.", obj.getMetadata().getNamespace(), obj.getKind(),
        obj.getMetadata().getName());
    return true;
  }
}

