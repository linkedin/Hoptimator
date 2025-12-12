package com.linkedin.hoptimator.planner;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcCatalogSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

import com.google.common.io.Resources;

import com.linkedin.hoptimator.catalog.Database;
import com.linkedin.hoptimator.catalog.DatabaseSchema;


/** A one-shot stateful object, which creates Pipelines from SQL. */
public class HoptimatorPlanner {

  private static final List<RelOptRule> RULE_SET = Arrays.asList(
      PruneEmptyRules.PROJECT_INSTANCE, PruneEmptyRules.FILTER_INSTANCE,
      CoreRules.PROJECT_TO_SEMI_JOIN,
      //CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN, // not in this version of calcite
      CoreRules.JOIN_TO_SEMI_JOIN, CoreRules.MATCH, CoreRules.PROJECT_MERGE, CoreRules.FILTER_MERGE,
      CoreRules.AGGREGATE_STAR_TABLE, CoreRules.AGGREGATE_PROJECT_STAR_TABLE, CoreRules.FILTER_SCAN,
      CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC, CoreRules.FILTER_PROJECT_TRANSPOSE,
      CoreRules.PROJECT_CALC_MERGE, CoreRules.FILTER_CALC_MERGE, CoreRules.CALC_MERGE, CoreRules.FILTER_INTO_JOIN,
      CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES, CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
      CoreRules.FILTER_AGGREGATE_TRANSPOSE, CoreRules.JOIN_COMMUTE, JoinPushThroughJoinRule.RIGHT,
      JoinPushThroughJoinRule.LEFT, CoreRules.SORT_PROJECT_TRANSPOSE);

  /** HoptimatorPlanner is a one-shot stateful object, so we construct them via factories */
  public interface Factory {
    HoptimatorPlanner makePlanner() throws Exception;

    static Factory fromSchema(String catalog, Schema schema) {
      return () -> HoptimatorPlanner.fromSchema(catalog, schema);
    }

    static Factory fromDataSource(String catalog, DataSource dataSource) {
      return () -> HoptimatorPlanner.fromDataSource(catalog, dataSource);
    }

    static Factory fromJdbc(String url, String catalog, String username, String password) {
      return () -> HoptimatorPlanner.fromJdbc(url, catalog, username, password);
    }

    static Factory fromJdbc(String url, Properties properties) {
      return () -> HoptimatorPlanner.fromJdbc(url, properties);
    }
  }

  private final FrameworkConfig calciteFrameworkConfig;
  private final SchemaPlus schema;

  public HoptimatorPlanner(SchemaPlus schema) {
    this.schema = schema;
    List<RelTraitDef> traitDefs = new ArrayList<>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    this.calciteFrameworkConfig = Frameworks.newConfigBuilder()
        .defaultSchema(schema)
        .traitDefs(traitDefs)
        .ruleSets(new RuleSet[]{RuleSets.ofList(RULE_SET)})
        .build();
  }

  public PipelineRel pipeline(String sql) throws Exception {
    Planner planner = Frameworks.getPlanner(calciteFrameworkConfig);
    SqlNode parsed = planner.parse(sql);
    SqlNode validated = planner.validate(parsed);
    RelNode logicalPlan = planner.rel(validated).project();
    RelTraitSet traitSet = logicalPlan.getTraitSet();
    traitSet = traitSet.simplify();
    PipelineRel pipelineRel = (PipelineRel) planner.transform(0, traitSet.replace(PipelineRel.CONVENTION), logicalPlan);
    planner.close();
    return pipelineRel;
  }

  public RelNode logical(String sql) throws Exception {
    Planner planner = Frameworks.getPlanner(calciteFrameworkConfig);
    SqlNode parsed = planner.parse(sql);
    SqlNode validated = planner.validate(parsed);
    RelNode logicalPlan = planner.rel(validated).project();
    planner.close();
    return logicalPlan;
  }

  public Database database(String name) {
    String rootName = schema.getName();
    if (rootName == null || rootName.isEmpty()) {
      rootName = "ROOT";
    }
    Schema subSchema = schema.subSchemas().get(name);
    if (subSchema == null) {
      throw new NoSuchElementException("No database '" + name + "' found in schema '" + rootName + "'");
    }
    subSchema = ((SchemaPlus) subSchema).unwrap(DatabaseSchema.class);
    if (subSchema == null) {
      throw new IllegalArgumentException("No database '" + name + "' found in schema '" + rootName
          + "'. A sub-schema was found, but it is null");
    }
    return ((DatabaseSchema) subSchema).database();
  }

  public static HoptimatorPlanner fromSchema(String name, Schema schema) {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add(name == null ? "ROOT" : name, schema);
    return new HoptimatorPlanner(rootSchema);
  }

  public static HoptimatorPlanner fromDataSource(String catalog, DataSource dataSource) {
    Schema schema = JdbcCatalogSchema.create(null, catalog, dataSource, catalog);
    return fromSchema(catalog, schema);
  }

  public static HoptimatorPlanner fromModelFile(String filePath, Properties properties)
      throws SQLException, IOException {
    Driver driver = new Driver();
    CalciteConnection connection = (CalciteConnection) driver.connect("jdbc:calcite:", properties);
    SchemaPlus schema = connection.getRootSchema();
    if (!new File(filePath).isFile()) {
      URL url = Resources.getResource(filePath);
      String text = Resources.toString(url, StandardCharsets.UTF_8);
      // ModelHandler can't directly read from resources, so we use "inline:" to load it dynamically
      new ModelHandler(connection, "inline:" + text); // side-effect: modifies connection
    } else {
      new ModelHandler(connection, filePath); // side-effect: modifies connection
    }
    return new HoptimatorPlanner(schema);
  }

  public static HoptimatorPlanner fromJdbc(String url, String catalog, String username, String password) {
    DataSource dataSource = JdbcSchema.dataSource(url, null, username, password);
    return fromDataSource(catalog, dataSource);
  }

  public static HoptimatorPlanner fromJdbc(String url, Properties properties) throws SQLException, IOException {
    if (url.startsWith("jdbc:calcite:model=")) {
      return fromModelFile(url.substring("jdbc:calcite:model=".length()), properties);
    } else {
      return fromJdbc(url, properties.getProperty("catalog"), properties.getProperty("username"),
          properties.getProperty("password"));
    }
  }
}
