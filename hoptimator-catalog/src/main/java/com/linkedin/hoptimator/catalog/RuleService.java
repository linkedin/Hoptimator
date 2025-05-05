package com.linkedin.hoptimator.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;


public final class RuleService {

  private RuleService() {
  }

  public static Collection<RuleProvider> providers() {
    ServiceLoader<RuleProvider> loader = ServiceLoader.load(RuleProvider.class);
    List<RuleProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(providers::add);
    return providers;
  }

  public static Collection<RelOptRule> rules() {
    return providers().stream().flatMap(x -> x.rules().stream()).collect(Collectors.toList());
  }
}
