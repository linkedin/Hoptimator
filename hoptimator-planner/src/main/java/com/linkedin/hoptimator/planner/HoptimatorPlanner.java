package com.linkedin.hoptimator.planner;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;

import com.linkedin.hoptimator.catalog.AdapterService;
import com.linkedin.hoptimator.catalog.AdapterSchema;

import java.util.Properties;
import java.util.List;
import java.util.ArrayList;

/** A one-shot stateful object, which creates Pipelines from SQL. */
public class HoptimatorPlanner {

  /** HoptimatorPlanner is a one-shot stateful object, so we construct them via factories */
  public interface Factory {
    HoptimatorPlanner makePlanner() throws Exception;

    static Factory create() {
      return () -> HoptimatorPlanner.create();
    }
  }

  private final FrameworkConfig calciteFrameworkConfig;

  public HoptimatorPlanner(SchemaPlus schema) {
    List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    this.calciteFrameworkConfig = Frameworks.newConfigBuilder()
        .defaultSchema(schema)
        .traitDefs(traitDefs)
        .ruleSets(new RuleSet[]{RuleSets.ofList(PipelineRules.RULE_SET)})
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
    RelTraitSet traitSet = logicalPlan.getTraitSet();
    planner.close();
    return logicalPlan;
  }

  // for testing purposes
  static HoptimatorPlanner fromModelFile(String filePath, Properties properties) throws Exception {
    String uri = filePath;
    if (uri.startsWith("jdbc:calcite:model=")) {
      uri = uri.substring("jdbc:calcite:model=".length());
    }
    Driver driver = new Driver();
    CalciteConnectionConfig connectionConfig = new CalciteConnectionConfigImpl(properties);
    CalciteConnection connection = (CalciteConnection) driver.connect("jdbc:calcite:", properties);
    SchemaPlus schema = connection.getRootSchema();
    ModelHandler modelHandler = new ModelHandler(connection, uri); // side-effect: modifies connection
    return new HoptimatorPlanner(schema);
  }

  // for testing purposes
  static HoptimatorPlanner fromSchema(String name, Schema schema) {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add(name, schema);
    return new HoptimatorPlanner(rootSchema);
  }

  public static HoptimatorPlanner create() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    AdapterService.adapters().stream().forEach(x -> rootSchema.add(x.database(), new AdapterSchema(x.database(), x)));
    return new HoptimatorPlanner(rootSchema);
  }
}
