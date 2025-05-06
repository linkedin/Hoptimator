package com.linkedin.hoptimator.util.planner;

import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import com.linkedin.hoptimator.Engine;


/** Calling convention using a remote execution engine. */
public final class EngineRules {

  private final Engine engine;

  public EngineRules(Engine engine) {
    this.engine = engine;
  }

  public void register(HoptimatorJdbcConvention inTrait, RelOptPlanner planner,
      Properties connectionProperties) {
    RemoteConvention remote = inTrait.remoteConventionForEngine(engine);
    planner.addRule(RemoteToEnumerableConverterRule.create(remote, connectionProperties));
    planner.addRule(RemoteJoinRule.Config.INSTANCE
        .withConversion(PipelineRules.PipelineJoin.class, PipelineRel.CONVENTION, remote, "RemoteJoinRule")
        .withRuleFactory(RemoteJoinRule::new)
        .as(RemoteJoinRule.Config.class)
        .toRule(RemoteJoinRule.class));
    planner.addRule(RemoteTableScanRule.Config.INSTANCE
        .withConversion(PipelineRules.PipelineTableScan.class, PipelineRel.CONVENTION, remote, "RemoteTableScan")
        .withRuleFactory(RemoteTableScanRule::new)
        .as(RemoteTableScanRule.Config.class)
        .toRule(RemoteTableScanRule.class));
  }

  private static class RemoteTableScanRule extends ConverterRule {

    protected RemoteTableScanRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      TableScan scan = (TableScan) rel;
      RelOptTable relOptTable = scan.getTable();
      RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutTrait());
      return new RemoteTableScan(rel.getCluster(), newTraitSet, relOptTable);
    }
  }

  private static class RemoteTableScan extends TableScan implements RemoteRel {

    RemoteTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
      super(cluster, traitSet, Collections.emptyList(), table);
    }
  }

  private class RemoteJoinRule extends ConverterRule {

    protected RemoteJoinRule(Config config) {
      super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
      RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutTrait());
      Join join = (Join) rel;
      try {
        return new RemoteJoin(rel.getCluster(), newTraitSet, join.getLeft(), join.getRight(),
            join.getCondition(), join.getJoinType());
      } catch (InvalidRelException e) {
        throw new AssertionError(e);
      }
    }
  }

  private class RemoteJoin extends Join implements RemoteRel {

    protected RemoteJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left,
        RelNode right, RexNode condition, JoinRelType joinType)
        throws InvalidRelException {
      super(cluster, traitSet, Collections.emptyList(), left, right, condition,
          Collections.emptySet(), joinType);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right,
        JoinRelType joinType, boolean semiJoinDone) {
      try {
        return new RemoteJoin(getCluster(), traitSet, left, right, condition, joinType);
      } catch (InvalidRelException e) {
        throw new AssertionError(e);
      }
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return Objects.requireNonNull(super.computeSelfCost(planner, mq)).multiplyBy(0);  // TODO fix zero cost
    }
  }

  private static SqlDialect dialect(Engine engine) {
    switch (engine.dialect()) {
      case ANSI:
        return AnsiSqlDialect.DEFAULT;
      case FLINK:
        return MysqlSqlDialect.DEFAULT;
      default:
        throw new IllegalArgumentException("Unknown dialect " + engine.dialect());
    }
  }
}
