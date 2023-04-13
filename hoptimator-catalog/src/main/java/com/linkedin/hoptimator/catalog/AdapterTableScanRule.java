package com.linkedin.hoptimator.catalog;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

class AdapterTableScanRule extends ConverterRule {
  public static final AdapterTableScanRule INSTANCE = Config.INSTANCE
    .withConversion(LogicalTableScan.class, Convention.NONE,
        AdapterRel.CONVENTION, "AdapterTableScanRule")
    .withRuleFactory(AdapterTableScanRule::new)
    .as(Config.class)
    .toRule(AdapterTableScanRule.class);

  protected AdapterTableScanRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalTableScan scan = (LogicalTableScan) rel;
    RelTraitSet traitSet = scan.getTraitSet().replace(AdapterRel.CONVENTION);
    return new AdapterTableScan(rel.getCluster(), traitSet, scan.getTable());
  }
}
