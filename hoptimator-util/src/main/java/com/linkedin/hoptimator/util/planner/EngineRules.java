package com.linkedin.hoptimator.util.planner;

import java.util.Collections;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcRel;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import com.linkedin.hoptimator.Engine;


/** Calling convention using a remote execution engine. */
public final class EngineRules {

  private final Engine engine;

  public EngineRules(Engine engine) {
    this.engine = engine;
  }

  public void register(HoptimatorJdbcConvention inTrait, RelOptPlanner planner) {
    SqlDialect dialect = dialect(engine);
    String name = engine.engineName() + "-" + inTrait.database();
    JdbcConvention outTrait = JdbcConvention.of(dialect, inTrait.expression, name);
 
    System.out.println("Registering rules for " + name + " using dialect " + dialect.toString());
    planner.addRule(RemoteJoinRule.Config.INSTANCE
        .withConversion(Join.class, Convention.NONE, outTrait, "RemoteJoinRule")
        .withRuleFactory(RemoteJoinRule::new)
        .as(RemoteJoinRule.Config.class)
        .toRule(RemoteJoinRule.class));
    planner.addRule(RemoteTableScanRule.Config.INSTANCE
        .withConversion(TableScan.class, Convention.NONE, outTrait, "RemoteTableScan")
        .withRuleFactory(RemoteTableScanRule::new)
        .as(RemoteTableScanRule.Config.class)
        .toRule(RemoteTableScanRule.class));
  }

  private class RemoteTableScanRule extends ConverterRule {

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

  private class RemoteTableScan extends TableScan implements JdbcRel {

    public RemoteTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
      super(cluster, traitSet, Collections.emptyList(), table);
    }

    @Override
    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      SqlDialect dialect = dialect(engine);
      System.out.println("Generating sql in dialect " + dialect.toString());
      JdbcImplementor.Result res = new JdbcImplementor(dialect, new JavaTypeFactoryImpl()).implement(getInput(0));
      System.out.println("Implemented: " + res.toString());
      return res;
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
        System.out.println(e);
        throw new AssertionError(e);
      }
    }
  }

  private class RemoteJoin extends Join implements JdbcRel {

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
        System.out.println(e);
        throw new AssertionError(e);
      }
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return super.computeSelfCost(planner, mq).multiplyBy(0);  // TODO fix zero cost
    }

    @Override
    public JdbcImplementor.Result implement(JdbcImplementor implementor) {
      SqlDialect dialect = dialect(engine);
      System.out.println("Generating sql in dialect " + dialect.toString());
      JdbcImplementor.Result res = new JdbcImplementor(dialect,
          new JavaTypeFactoryImpl()).implement(getInput(0));
      System.out.println("implemented (2) " + res.toString());
      return res;
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
