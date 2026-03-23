package com.linkedin.hoptimator.util.planner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class PipelineRulesTest {

  @Test
  void testRulesReturnsExpectedCount() {
    Collection<RelOptRule> rules = PipelineRules.rules();

    assertNotNull(rules);
    assertEquals(6, rules.size());
  }

  @Test
  void testFindTableSingleName() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("myTable", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return rowType;
      }
    });

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    Table found = PipelineRules.findTable(calciteSchema, "myTable");

    assertNotNull(found);
  }

  @Test
  void testFindTableQualifiedName() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sub = rootSchema.add("db", new AbstractSchema());
    sub.add("tbl", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return rowType;
      }
    });

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    Table found = PipelineRules.findTable(calciteSchema, Arrays.asList("db", "tbl"));

    assertNotNull(found);
  }

  @Test
  void testFindTableEmptyNameThrows() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);

    assertThrows(IllegalArgumentException.class,
        () -> PipelineRules.findTable(calciteSchema, Collections.emptyList()));
  }

  @Test
  void testFindTableMissingTableThrows() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);

    assertThrows(IllegalArgumentException.class,
        () -> PipelineRules.findTable(calciteSchema, "nonexistent"));
  }

  @Test
  void testFindTableMissingSchemaThrows() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);

    assertThrows(IllegalArgumentException.class,
        () -> PipelineRules.findTable(calciteSchema, Arrays.asList("nonexistent", "tbl")));
  }

  @Test
  void testQualifiedNameFromRelNode() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sub = rootSchema.add("S", new AbstractSchema());
    sub.add("T", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return rowType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
    RelNode scan = builder.scan("S", "T").build();

    List<String> qn = PipelineRules.qualifiedName(scan);
    assertNotNull(qn);
    assertEquals("T", qn.get(qn.size() - 1));

    String name = PipelineRules.name(scan);
    assertEquals("T", name);
  }

  @Test
  void testQualifiedNameFromRelOptTable() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sub = rootSchema.add("S", new AbstractSchema());
    sub.add("T", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return rowType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
    RelNode scan = builder.scan("S", "T").build();

    RelOptTable table = scan.getTable();
    List<String> qn = PipelineRules.qualifiedName(table);
    assertNotNull(qn);
  }

  @Test
  void testSchemaReturnsCalciteSchema() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sub = rootSchema.add("S", new AbstractSchema());
    sub.add("T", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return rowType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
    RelNode scan = builder.scan("S", "T").build();

    CalciteSchema schema = PipelineRules.schema(scan);
    assertNotNull(schema);
  }

  @Mock
  private RelNode mockRelNode;

  @Test
  void testSchemaThrowsOnNullTable() {
    when(mockRelNode.getTable()).thenReturn(null);

    assertThrows(IllegalArgumentException.class, () -> PipelineRules.schema(mockRelNode));
  }

  @Test
  void testQualifiedNameThrowsOnNullTable() {
    when(mockRelNode.getTable()).thenReturn(null);

    assertThrows(IllegalArgumentException.class, () -> PipelineRules.qualifiedName(mockRelNode));
  }
}
