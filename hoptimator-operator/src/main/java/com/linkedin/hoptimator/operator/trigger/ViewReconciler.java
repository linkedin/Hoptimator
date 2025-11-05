package com.linkedin.hoptimator.operator.trigger;

import java.sql.SQLException;
import java.time.Duration;

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
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewStatus;


/**
 * Listens to View change events and fires any matching TableTriggers.
 *
 */
public final class ViewReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(ViewReconciler.class);
  private final static String VIEW_KEY = "view";

  private final K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi;
  private final K8sApi<V1alpha1View, V1alpha1ViewList> viewApi;

  private ViewReconciler(K8sContext context) {
    this(new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS),
        new K8sApi<>(context, K8sApiEndpoints.VIEWS));
  }

  ViewReconciler(K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> tableTriggerApi,
      K8sApi<V1alpha1View, V1alpha1ViewList> viewApi) {
    this.tableTriggerApi = tableTriggerApi;
    this.viewApi = viewApi;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    try {
      V1alpha1View object;
      try {
        object = viewApi.get(namespace, name);
      } catch (SQLException e) {
        if (e.getErrorCode() == 404) {
          log.info("Object {} deleted. Skipping.", name);
          return new Result(false);
        }
        throw e;
      }

      V1alpha1ViewStatus status = object.getStatus();
      if (status != null && status.getWatermark() != null) {
        log.info("View {} was updated at {}.", name, status.getWatermark());
        for (V1alpha1TableTrigger trigger : tableTriggerApi.select(VIEW_KEY + "=" + name)) {
          log.info("Firing downstream trigger {}.", trigger.getMetadata().getName());
          V1alpha1TableTriggerStatus triggerStatus = trigger.getStatus();
          if (triggerStatus == null) {
            triggerStatus = new V1alpha1TableTriggerStatus();
          }
          triggerStatus.setTimestamp(status.getWatermark());
          trigger.setStatus(triggerStatus);
          tableTriggerApi.updateStatus(trigger, triggerStatus);
        }
      }
      return new Result(false);
    } catch (Exception e) {
      log.error("Encountered exception while reconciling View {}.", name, e);
      return new Result(true, failureRetryDuration());
    }
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
    Reconciler reconciler = new ViewReconciler(context);
    return ControllerBuilder.defaultBuilder(context.informerFactory())
        .withReconciler(reconciler)
        .withName("view-trigger-controller")
        .withWorkerCount(1)
        .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1View.class, x).build())
        .build();
  }
}

