package com.linkedin.hoptimator.catalog;

import java.util.Collections;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;


/** Internal. */
public final class HopTableScan extends TableScan implements HopRel {

  HopTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, Collections.emptyList(), table);
    assert getConvention() == HopRel.CONVENTION;
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(HopTableScanRule.INSTANCE);
    RuleService.rules().forEach(x -> planner.addRule(x));
  }
}

