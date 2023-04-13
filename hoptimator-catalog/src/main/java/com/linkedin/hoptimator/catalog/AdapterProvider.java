package com.linkedin.hoptimator.catalog;

import org.apache.calcite.plan.RelOptRule;

import java.util.Collection;
import java.util.Collections;

/** Service Provider Interface for extending Hoptimator via Adapters. */
public interface AdapterProvider {

  /** Provided adapters */
  Collection<Adapter> adapters();
 
  /** Additional rules to expose to the planner */ 
  default Collection<RelOptRule> rules() {
    return Collections.emptyList();
  }
}
