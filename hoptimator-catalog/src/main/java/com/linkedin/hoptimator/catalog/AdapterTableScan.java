package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;

import java.util.Collections;

/** Internal. */
public final class AdapterTableScan extends TableScan implements AdapterRel {

  AdapterTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, Collections.emptyList(), table);
    assert getConvention() == AdapterRel.CONVENTION;
  }

  @Override
  public void register(RelOptPlanner planner) {
    AdapterService.registerRules(planner);
  }
}

