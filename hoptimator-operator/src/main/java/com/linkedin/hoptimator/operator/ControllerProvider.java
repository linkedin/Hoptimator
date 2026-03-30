package com.linkedin.hoptimator.operator;

import com.linkedin.hoptimator.k8s.K8sContext;
import io.kubernetes.client.extended.controller.Controller;

import java.util.Collection;


/** Service Provider Interface to enable dynamically loading Controllers. */
public interface ControllerProvider {

  Collection<Controller> controllers(K8sContext context);
}
