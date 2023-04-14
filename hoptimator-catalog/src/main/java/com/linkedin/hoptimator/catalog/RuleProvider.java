package com.linkedin.hoptimator.catalog;

import org.apache.calcite.plan.RelOptRule;

import java.util.Collection;

/** Service Provider Interface to enable dynamically loading planner rules. */
public interface RuleProvider {
  Collection<RelOptRule> rules();
}
