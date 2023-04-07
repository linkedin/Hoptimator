package com.linkedin.hoptimator.catalog;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.util.Iterator;
import java.util.Collection;
import java.util.ServiceLoader;

/** Enables loading planner rules dynamically via SPIs */
public interface AdapterRules {

  Collection<RelRule> rules();

  static Iterator<AdapterRules> providers() {
    ServiceLoader<AdapterRules> loader = ServiceLoader.load(AdapterRules.class);
    return loader.iterator();
  }

  static void registerRules(RelOptPlanner planner) {
    planner.addRule(AdapterTableScanRule.INSTANCE);
    Iterator<AdapterRules> iter = providers();
    int i = 0;
    while (iter.hasNext()) {
      i++;
      iter.next().rules().forEach(x -> {
        planner.addRule(x);
      });
    }
  }

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
}
