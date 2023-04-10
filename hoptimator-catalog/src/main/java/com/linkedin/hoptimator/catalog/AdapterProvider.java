package com.linkedin.hoptimator.catalog;

import org.apache.calcite.plan.RelOptRule;

import java.util.Collection;
import java.util.Collections;

/** Service Provider Interface for extending Hoptimator via Adapters. */
public interface AdapterProvider {
  Collection<Adapter> adapters();
  
  default Collection<RelOptRule> rules() {
    return Collections.emptyList();
  }
}
