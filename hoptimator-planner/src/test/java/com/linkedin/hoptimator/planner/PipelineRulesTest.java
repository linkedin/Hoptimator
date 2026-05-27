package com.linkedin.hoptimator.planner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class PipelineRulesTest {

  @Test
  public void rulesReturnsFiveRules() {
    PipelineRules pipelineRules = new PipelineRules();
    Collection<RelOptRule> rules = pipelineRules.rules();

    assertNotNull(rules);
    assertEquals(5, rules.size());
  }

  @Test
  public void rulesContainsExpectedRuleInstances() {
    PipelineRules pipelineRules = new PipelineRules();
    Collection<RelOptRule> rules = pipelineRules.rules();

    // Verify rules include the static instances
    assertTrue(rules.contains(PipelineRules.PipelineTableScanRule.INSTANCE));
    assertTrue(rules.contains(PipelineRules.PipelineFilterRule.INSTANCE));
    assertTrue(rules.contains(PipelineRules.PipelineProjectRule.INSTANCE));
    assertTrue(rules.contains(PipelineRules.PipelineJoinRule.INSTANCE));
    assertTrue(rules.contains(PipelineRules.PipelineCalcRule.INSTANCE));
  }

  @Test
  public void findTableFindsTableByName() {
    CalciteSchema schema = CalciteSchema.createRootSchema(false);
    Table mockTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT).createStructType(
            Collections.emptyList(), Collections.emptyList());
      }
    };
    schema.add("MY_TABLE", mockTable);

    Table found = PipelineRules.findTable(schema, "MY_TABLE");
    assertNotNull(found);
    assertEquals(mockTable, found);
  }

  @Test
  public void findTableThrowsWhenTableNotFound() {
    CalciteSchema schema = CalciteSchema.createRootSchema(false);

    assertThrows(IllegalArgumentException.class, () -> PipelineRules.findTable(schema, "NONEXISTENT"));
  }

  @Test
  public void findTableWithQualifiedNameFindsTableInSubSchema() {
    CalciteSchema root = CalciteSchema.createRootSchema(false);
    CalciteSchema subSchema = root.add("MY_SCHEMA", new AbstractSchema() {
    });

    Table mockTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT).createStructType(
            Collections.emptyList(), Collections.emptyList());
      }
    };
    subSchema.add("MY_TABLE", mockTable);

    Table found = PipelineRules.findTable(root, Arrays.asList("MY_SCHEMA", "MY_TABLE"));
    assertNotNull(found);
    assertEquals(mockTable, found);
  }

  @Test
  public void findTableWithQualifiedNameThrowsWhenSubSchemaNotFound() {
    CalciteSchema root = CalciteSchema.createRootSchema(false);

    assertThrows(IllegalArgumentException.class,
        () -> PipelineRules.findTable(root, Arrays.asList("NONEXISTENT_SCHEMA", "MY_TABLE")));
  }

  @Test
  public void findTableWithEmptyQualifiedNameThrows() {
    CalciteSchema schema = CalciteSchema.createRootSchema(false);

    assertThrows(IllegalArgumentException.class,
        () -> PipelineRules.findTable(schema, Collections.emptyList()));
  }

  @Test
  public void findTableWithSingleElementQualifiedNameFindsTable() {
    CalciteSchema schema = CalciteSchema.createRootSchema(false);
    Table mockTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT).createStructType(
            Collections.emptyList(), Collections.emptyList());
      }
    };
    schema.add("MY_TABLE", mockTable);

    Table found = PipelineRules.findTable(schema, Collections.singletonList("MY_TABLE"));
    assertNotNull(found);
    assertEquals(mockTable, found);
  }

  @Test
  public void pipelineRelConventionIsNamed() {
    assertNotNull(PipelineRel.CONVENTION);
    assertEquals("PIPELINE", PipelineRel.CONVENTION.getName());
  }
}
