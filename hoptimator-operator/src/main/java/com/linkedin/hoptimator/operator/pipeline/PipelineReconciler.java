package com.linkedin.hoptimator.operator.pipeline;

import java.sql.SQLException;
import java.time.Duration;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineStatus;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatusEstimator;


/**
 * Manages Pipelines.
 */
public final class PipelineReconciler implements Reconciler {
  private static final Logger log = LoggerFactory.getLogger(PipelineReconciler.class);

  private final K8sContext context;
  private final K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi;
  private final K8sPipelineElementStatusEstimator elementStatusEstimator;

  private PipelineReconciler(K8sContext context) {
    this.context = context;
    this.pipelineApi = new K8sApi<>(context, K8sApiEndpoints.PIPELINES);
    this.elementStatusEstimator = new K8sPipelineElementStatusEstimator(context);
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    Result result = new Result(true, pendingRetryDuration());
    try {
      V1alpha1Pipeline object;
      try {
        object = pipelineApi.get(namespace, name);
      } catch (SQLException e) {
        if (e.getErrorCode() == 404) {
          log.info("Object {} deleted. Skipping.", name);
          return new Result(false);
        }
        throw e;
      }

      V1alpha1PipelineStatus status = object.getStatus();
      if (status == null) {
        status = new V1alpha1PipelineStatus();
        object.setStatus(status);
      }

      log.info("Checking status of Pipeline {}...", name);

      List<K8sPipelineElementStatus> elementStatuses = elementStatusEstimator.estimateStatuses(object);
      boolean ready = elementStatuses.stream().allMatch(K8sPipelineElementStatus::isReady);
      boolean failed = elementStatuses.stream().allMatch(K8sPipelineElementStatus::isFailed);

      if (ready) {
        status.setReady(true);
        status.setFailed(false);
        status.setMessage("Ready.");
        log.info("Pipeline {} is ready.", name);
        result = new Result(false);
      } else if (failed) {
        status.setReady(false);
        status.setFailed(true);
        status.setMessage("Failed.");
        log.info("Pipeline {} failed.", name);
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
  private Duration failureRetryDuration() {
    return Duration.ofMinutes(5);
  }

  // TODO load from configuration
  private Duration pendingRetryDuration() {
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
}

