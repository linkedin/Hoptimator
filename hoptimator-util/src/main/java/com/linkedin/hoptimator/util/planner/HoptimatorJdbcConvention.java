package com.linkedin.hoptimator.util.planner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.sql.SqlDialect;

import com.linkedin.hoptimator.Engine;


public class HoptimatorJdbcConvention extends JdbcConvention {

  private final String database;
  private final List<Engine> engines;
  private final Properties connectionProperties;
  private final Map<String, RemoteConvention> remoteConventions = new HashMap<>();

  public HoptimatorJdbcConvention(SqlDialect dialect, Expression expression, String name,
      List<Engine> engines, Properties connectionProperties) {
    super(dialect, expression, name);
    this.database = name;
    this.engines = engines;
    this.connectionProperties = connectionProperties;
  }

  public String database() {
    return database;
  }

  public List<Engine> engines() {
    return engines;
  }

  public RemoteConvention remoteConventionForEngine(Engine engine) {
    return remoteConventions.computeIfAbsent(engine.engineName(), x -> new RemoteConvention(
        x + "-" + database, engine));
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    planner.addRule(PipelineRules.PipelineTableScanRule.create(this));
    planner.addRule(PipelineRules.PipelineTableModifyRule.create(this));
    PipelineRules.rules().forEach(planner::addRule);
    engines().forEach(x -> new EngineRules(x).register(this, planner, connectionProperties));
  }
}
