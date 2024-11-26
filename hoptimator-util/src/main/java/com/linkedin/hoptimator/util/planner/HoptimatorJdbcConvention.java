package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.sql.SqlDialect;

public class HoptimatorJdbcConvention extends JdbcConvention {

  private final String database;

  public HoptimatorJdbcConvention(SqlDialect dialect, Expression expression, String name) {
    super(dialect, expression, name);
    this.database = name;
  }

  public String database() {
    return database;
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    planner.addRule(PipelineRules.PipelineTableScanRule.create(this));
    planner.addRule(PipelineRules.PipelineTableModifyRule.create(this));
  }
}
