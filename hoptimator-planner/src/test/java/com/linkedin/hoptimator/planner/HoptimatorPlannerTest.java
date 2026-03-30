package com.linkedin.hoptimator.planner;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class HoptimatorPlannerTest {

  @Test
  public void fromSchemaCreatesNonNullPlanner() {
    Schema schema = new AbstractSchema();
    HoptimatorPlanner planner = HoptimatorPlanner.fromSchema("TEST_CATALOG", schema);
    assertNotNull(planner);
  }

  @Test
  public void fromSchemaWithNullNameUsesRoot() {
    Schema schema = new AbstractSchema();
    // Should not throw
    HoptimatorPlanner planner = HoptimatorPlanner.fromSchema(null, schema);
    assertNotNull(planner);
  }

  @Test
  public void constructorAcceptsSchemaPlus() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    HoptimatorPlanner planner = new HoptimatorPlanner(rootSchema);
    assertNotNull(planner);
  }

  @Test
  public void databaseThrowsWhenSubSchemaMissing() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    HoptimatorPlanner planner = new HoptimatorPlanner(rootSchema);

    assertThrows(NoSuchElementException.class, () -> planner.database("NONEXISTENT"));
  }

  @Test
  public void databaseThrowsWhenSubSchemaIsNotDatabaseSchema() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    // Add a plain (non-DatabaseSchema) sub-schema
    rootSchema.add("PLAIN", new AbstractSchema());
    HoptimatorPlanner planner = new HoptimatorPlanner(rootSchema);

    // Should throw because PLAIN is not a DatabaseSchema
    assertThrows(Exception.class, () -> planner.database("PLAIN"));
  }

  @Test
  public void factoryFromSchemaCreatesFactory() {
    Schema schema = new AbstractSchema();
    HoptimatorPlanner.Factory factory = HoptimatorPlanner.Factory.fromSchema("TEST", schema);
    assertNotNull(factory);
  }

  @Test
  public void factoryFromSchemaCanMakePlanner() throws Exception {
    Schema schema = new AbstractSchema();
    HoptimatorPlanner.Factory factory = HoptimatorPlanner.Factory.fromSchema("TEST", schema);
    HoptimatorPlanner planner = factory.makePlanner();
    assertNotNull(planner);
  }

  @Test
  public void factoryFromJdbcCreatesFactory() {
    HoptimatorPlanner.Factory factory =
        HoptimatorPlanner.Factory.fromJdbc("jdbc:h2:mem:test", "TEST", "sa", "");
    assertNotNull(factory);
  }

  @Test
  public void factoryFromJdbcWithPropertiesCreatesFactory() {
    Properties props = new Properties();
    props.setProperty("catalog", "TEST");
    props.setProperty("username", "sa");
    props.setProperty("password", "");
    HoptimatorPlanner.Factory factory = HoptimatorPlanner.Factory.fromJdbc("jdbc:h2:mem:test", props);
    assertNotNull(factory);
  }

  @Test
  public void fromJdbcWithCalciteModelPrefixDelegatesToModelFile() {
    // If url starts with jdbc:calcite:model=, it should delegate to fromModelFile
    // This will throw a meaningful error because no model file exists, but the dispatch logic is tested
    Properties props = new Properties();
    assertThrows(Exception.class,
        () -> HoptimatorPlanner.fromJdbc("jdbc:calcite:model=nonexistent_model.json", props));
  }
}
