package com.linkedin.hoptimator.operator;

import com.linkedin.hoptimator.k8s.K8sContext;
import io.kubernetes.client.extended.controller.Controller;

import java.util.Collection;
import java.util.Collections;


/** Test implementation of ControllerProvider for use in ServiceLoader-based tests. */
public class TestControllerProvider implements ControllerProvider {

  @Override
  public Collection<Controller> controllers(K8sContext context) {
    // Return an empty collection — we just need the provider to be registered
    return Collections.emptyList();
  }
}
