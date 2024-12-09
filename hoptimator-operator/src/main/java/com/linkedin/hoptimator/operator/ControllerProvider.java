package com.linkedin.hoptimator.operator;

import java.util.Collection;

import io.kubernetes.client.extended.controller.Controller;


/** Service Provider Interface to enable dynamically loading Controllers. */
public interface ControllerProvider {

  Collection<Controller> controllers(Operator operator);
}
