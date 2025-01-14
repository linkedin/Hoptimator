package com.linkedin.hoptimator.util.planner;

import java.util.List;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.sql.SqlDialect;

import com.linkedin.hoptimator.Engine;


public class HoptimatorJdbcConvention extends JdbcConvention {

  private final String database;
  private final List<Engine> engines;

  public HoptimatorJdbcConvention(SqlDialect dialect, Expression expression, String name,
      List<Engine> engines) {
    super(dialect, expression, name);
    this.database = name;
    this.engines = engines;
  }

  public String database() {
    return database;
  }

  public List<Engine> engines() {
    return engines;
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    planner.addRule(PipelineRules.PipelineTableScanRule.create(this));
    planner.addRule(PipelineRules.PipelineTableModifyRule.create(this));
    engines().forEach(x -> new EngineRules(x).register(this, planner));
  }
}
