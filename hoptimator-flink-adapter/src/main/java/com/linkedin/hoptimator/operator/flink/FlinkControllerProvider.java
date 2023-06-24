package com.linkedin.hoptimator.operator.flink;

import com.linkedin.hoptimator.operator.ControllerProvider;
import com.linkedin.hoptimator.operator.Operator;

import io.kubernetes.client.extended.controller.Controller;

import java.util.Collection;
import java.util.Collections;

/** A bridge to flink-kubernetes-operator */
public class FlinkControllerProvider implements ControllerProvider {

  @Override
  public Collection<Controller> controllers(Operator operator) {
    operator.registerApi("FlinkDeployment", "flinkdeployment", "flinkdeployments",
      "flink.apache.org", "v1beta1");

    // We don't need a controller
    return Collections.emptyList();
  }
}
