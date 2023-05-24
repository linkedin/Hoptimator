package com.linkedin.hoptimator.operator.flink;

import com.linkedin.hoptimator.operator.ControllerProvider;
import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.models.V1alpha1SqlJob;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;

import java.util.Collection;
import java.util.Collections;

/** Provides a Controller plugin for FlinkDeployments. */
public class FlinkControllerProvider implements ControllerProvider {

  @Override
  public Collection<Controller> controllers(Operator operator) {
    operator.registerApi("FlinkDeployment", "flinkdeployment", "flinkdeployments",
      "flink.apache.org", "v1beta1");

    Reconciler reconciler = new FlinkSqlJobReconciler(operator);
    Controller controller = ControllerBuilder.defaultBuilder(operator.informerFactory())
      .withReconciler(reconciler)
      .withName("flink-deployment-controller")
      .withWorkerCount(1)
      .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1SqlJob.class, x).build())
      .build();

    return Collections.singleton(controller);
  }
}
