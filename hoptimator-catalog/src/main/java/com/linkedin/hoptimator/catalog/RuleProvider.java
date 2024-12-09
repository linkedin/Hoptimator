package com.linkedin.hoptimator.catalog;

import java.util.Collection;

import org.apache.calcite.plan.RelOptRule;


/** Service Provider Interface to enable dynamically loading planner rules. */
public interface RuleProvider {
  Collection<RelOptRule> rules();
}
