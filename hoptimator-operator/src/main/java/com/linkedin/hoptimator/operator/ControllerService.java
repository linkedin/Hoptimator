package com.linkedin.hoptimator.operator;

import com.linkedin.hoptimator.k8s.K8sContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import io.kubernetes.client.extended.controller.Controller;


/** Loads controller plugins via the ControllerProvider Service Provider Interface. */
public final class ControllerService {

  private ControllerService() {
  }

  public static Collection<ControllerProvider> providers() {
    ServiceLoader<ControllerProvider> loader = ServiceLoader.load(ControllerProvider.class);
    List<ControllerProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(providers::add);
    return providers;
  }

  public static Collection<Controller> controllers(K8sContext context) {
    return providers().stream().flatMap(x -> x.controllers(context).stream()).collect(Collectors.toList());
  }
}
