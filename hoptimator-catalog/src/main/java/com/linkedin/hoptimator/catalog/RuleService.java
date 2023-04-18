package com.linkedin.hoptimator.catalog;

import org.apache.calcite.plan.RelOptRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public final class RuleService {

  public static Collection<RuleProvider> providers() {
    ServiceLoader<RuleProvider> loader = ServiceLoader.load(RuleProvider.class);
    List<RuleProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(x -> providers.add(x));
    return providers;
  }

  public static Collection<RelOptRule> rules() {
    return providers().stream().flatMap(x -> x.rules().stream())
      .collect(Collectors.toList());
  }
}
