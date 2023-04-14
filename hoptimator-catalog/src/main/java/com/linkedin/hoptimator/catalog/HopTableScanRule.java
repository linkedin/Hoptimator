package com.linkedin.hoptimator.catalog;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

class HopTableScanRule extends ConverterRule {
  public static final HopTableScanRule INSTANCE = Config.INSTANCE
    .withConversion(LogicalTableScan.class, Convention.NONE,
        HopRel.CONVENTION, "HopTableScanRule")
    .withRuleFactory(HopTableScanRule::new)
    .as(Config.class)
    .toRule(HopTableScanRule.class);

  protected HopTableScanRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalTableScan scan = (LogicalTableScan) rel;
    RelTraitSet traitSet = scan.getTraitSet().replace(HopRel.CONVENTION);
    return new HopTableScan(rel.getCluster(), traitSet, scan.getTable());
  }
}
