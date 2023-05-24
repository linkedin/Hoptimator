package com.linkedin.hoptimator.operator;

import org.apache.calcite.plan.RelOptRule;

import io.kubernetes.client.extended.controller.Controller;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/** Loads controller plugins via the ControllerProvider Service Provider Interface. */
public final class ControllerService {

  public static Collection<ControllerProvider> providers() {
    ServiceLoader<ControllerProvider> loader = ServiceLoader.load(ControllerProvider.class);
    List<ControllerProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(x -> providers.add(x));
    return providers;
  }

  public static Collection<Controller> controllers(Operator operator) {
    return providers().stream().flatMap(x -> x.controllers(operator).stream())
      .collect(Collectors.toList());
  }
}
