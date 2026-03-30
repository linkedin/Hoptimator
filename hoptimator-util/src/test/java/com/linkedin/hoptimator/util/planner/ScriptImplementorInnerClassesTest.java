package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class ScriptImplementorInnerClassesTest {

  @Test
  void testEmptyScriptProducesEmptyOutput() {
    String sql = ScriptImplementor.empty().sql();

    assertEquals("", sql);
  }

  @Test
  void testCatalogImplementorWithNullCatalogProducesNothing() {
    String sql = ScriptImplementor.empty().catalog(null).sql();

    assertEquals("", sql);
  }

  @Test
  void testCatalogImplementorProducesCreateCatalog() {
    String sql = ScriptImplementor.empty().catalog("myCatalog").sql();

    assertTrue(sql.contains("CREATE CATALOG IF NOT EXISTS"));
    assertTrue(sql.contains("myCatalog"));
  }

  @Test
  void testDatabaseImplementorProducesCreateDatabase() {
    String sql = ScriptImplementor.empty().database("cat", "myDb").sql();

    assertTrue(sql.contains("CREATE DATABASE IF NOT EXISTS"));
    assertTrue(sql.contains("myDb"));
  }

  @Test
  void testDatabaseImplementorWithNullCatalog() {
    String sql = ScriptImplementor.empty().database(null, "myDb").sql();

    assertTrue(sql.contains("CREATE DATABASE IF NOT EXISTS"));
    assertTrue(sql.contains("myDb"));
  }

  @Test
  void testConnectorImplementorWithNullTypes() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.NULL))
        .build();

    String sql = ScriptImplementor.empty()
        .connector(null, "S", "T", rowType, Collections.emptyMap())
        .sql();

    // NULL fields are promoted to BYTES
    assertTrue(sql.contains("BYTES"));
  }

  @Test
  void testConnectorImplementorWithPrimaryKey() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("PRIMARY_KEY", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("VALUE", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    String sql = ScriptImplementor.empty()
        .connector(null, "S", "T", rowType, Collections.emptyMap())
        .sql();

    assertTrue(sql.contains("PRIMARY KEY (PRIMARY_KEY) NOT ENFORCED"));
  }

  @Test
  void testConnectorImplementorWithConfig() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    Map<String, String> config = new HashMap<>();
    config.put("connector", "kafka");
    config.put("topic", "my-topic");

    String sql = ScriptImplementor.empty()
        .connector(null, "S", "T", rowType, config)
        .sql();

    assertTrue(sql.contains("'connector'='kafka'"));
    assertTrue(sql.contains("'topic'='my-topic'"));
  }

  @Test
  void testConnectorWithMapType() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType mapType = typeFactory.createMapType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataType rowType = typeFactory.builder()
        .add("MY_MAP", mapType)
        .build();

    String sql = ScriptImplementor.empty()
        .connector(null, "S", "T", rowType, Collections.emptyMap())
        .sql();

    assertTrue(sql.contains("MAP<"));
  }

  @Test
  void testConnectorWithStructRowType() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType innerType = typeFactory.builder()
        .add("INNER_FIELD", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
    RelDataType rowType = typeFactory.builder()
        .add("NESTED", innerType)
        .build();

    String sql = ScriptImplementor.empty()
        .connector(null, "S", "T", rowType, Collections.emptyMap())
        .sql();

    assertTrue(sql.contains("ROW"));
    assertTrue(sql.contains("INNER_FIELD"));
  }

  @Test
  void testConnectorWithArrayOfStructType() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType innerType = typeFactory.builder()
        .add("FIELD1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
    RelDataType arrayType = typeFactory.createArrayType(innerType, -1);
    RelDataType rowType = typeFactory.builder()
        .add("MY_ARRAY", arrayType)
        .build();

    String sql = ScriptImplementor.empty()
        .connector(null, "S", "T", rowType, Collections.emptyMap())
        .sql();

    assertTrue(sql.contains("ARRAY"));
    assertTrue(sql.contains("FIELD1"));
  }

  @Test
  void testCompoundIdentifierWithAllParts() {
    String sql = new ScriptImplementor.CompoundIdentifierImplementor("cat", "sch", "tbl").sql();

    assertTrue(sql.contains("cat"));
    assertTrue(sql.contains("sch"));
    assertTrue(sql.contains("tbl"));
  }

  @Test
  void testCompoundIdentifierWithSchemaAndTable() {
    String sql = new ScriptImplementor.CompoundIdentifierImplementor(null, "sch", "tbl").sql();

    assertTrue(sql.contains("sch"));
    assertTrue(sql.contains("tbl"));
  }

  @Test
  void testCompoundIdentifierWithCatalogOnly() {
    String sql = new ScriptImplementor.CompoundIdentifierImplementor("cat", null, null).sql();

    assertTrue(sql.contains("cat"));
  }

  @Test
  void testCompoundIdentifierWithSchemaOnly() {
    String sql = new ScriptImplementor.CompoundIdentifierImplementor(null, "sch", null).sql();

    assertTrue(sql.contains("sch"));
  }

  @Test
  void testCompoundIdentifierWithTableOnly() {
    String sql = new ScriptImplementor.CompoundIdentifierImplementor(null, null, "tbl").sql();

    assertTrue(sql.contains("tbl"));
  }

  @Test
  void testCompoundIdentifierWithCatalogAndSchema() {
    String sql = new ScriptImplementor.CompoundIdentifierImplementor("cat", "sch", null).sql();

    assertTrue(sql.contains("cat"));
    assertTrue(sql.contains("sch"));
  }

  @Test
  void testCompoundIdentifierWithCatalogAndTable() {
    String sql = new ScriptImplementor.CompoundIdentifierImplementor("cat", null, "tbl").sql();

    assertTrue(sql.contains("cat"));
    assertTrue(sql.contains("tbl"));
  }

  @Test
  void testCompoundIdentifierAllNull() {
    String sql = new ScriptImplementor.CompoundIdentifierImplementor(null, null, null).sql();

    assertEquals("", sql);
  }

  @Test
  void testIdentifierImplementor() {
    String sql = new ScriptImplementor.IdentifierImplementor("myTable").sql();

    assertTrue(sql.contains("myTable"));
  }

  @Test
  void testSealFunctionForAnsiDialect() {
    ScriptImplementor impl = ScriptImplementor.empty().database(null, "db");

    String result = impl.seal().apply(com.linkedin.hoptimator.SqlDialect.ANSI);

    assertTrue(result.contains("CREATE DATABASE IF NOT EXISTS"));
  }

  @Test
  void testSealFunctionForFlinkDialect() {
    ScriptImplementor impl = ScriptImplementor.empty().database(null, "db");

    String result = impl.seal().apply(com.linkedin.hoptimator.SqlDialect.FLINK);

    assertTrue(result.contains("CREATE DATABASE IF NOT EXISTS"));
  }

  @Test
  void testConnectorWithNotNullableField() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType notNullVarchar = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
    RelDataType rowType = typeFactory.createStructType(
        Collections.singletonList(notNullVarchar),
        Collections.singletonList("COL1"));

    String sql = ScriptImplementor.empty()
        .connector(null, "S", "T", rowType, Collections.emptyMap())
        .sql();

    // The field should be NOT NULL since we specified non-nullable
    assertTrue(sql.contains("COL1"));
    assertTrue(sql.contains("VARCHAR"));
  }

  @Test
  void testViewImplementor() {
    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(Frameworks.createRootSchema(true))
            .build());
    RelNode values = builder.values(new String[]{"COL1"}, "val1").build();

    String sql = new ScriptImplementor.ViewImplementor("myView", values).sql();

    assertTrue(sql.contains("CREATE TEMPORARY VIEW"));
    assertTrue(sql.contains("myView"));
  }

  @Test
  void testStatementImplementor() {
    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(Frameworks.createRootSchema(true))
            .build());
    RelNode values = builder.values(new String[]{"COL1"}, "val1").build();

    String sql = new ScriptImplementor.StatementImplementor(values).sql();

    assertTrue(sql.contains("SELECT"));
  }

  @Test
  void testQueryImplementorWithTableNameReplacement() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus testSchema = rootSchema.add("TEST", new AbstractSchema());
    testSchema.add("MY_TABLE", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
    RelNode scan = builder.scan("TEST", "MY_TABLE").build();

    Map<String, String> replacements = new HashMap<>();
    replacements.put("TEST.MY_TABLE", "MY_TABLE_source");

    String sql = new ScriptImplementor.QueryImplementor(scan, replacements).sql();

    assertTrue(sql.contains("MY_TABLE_source"));
  }

  @Test
  void testInsertWithNullTargetFields() {
    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(Frameworks.createRootSchema(true))
            .build());
    RelNode values = builder.values(new String[]{"COL1"}, "val1").build();

    String sql = ScriptImplementor.empty()
        .insert(null, "S", "T", values)
        .sql();

    assertTrue(sql.contains("INSERT INTO"));
    assertTrue(sql.contains("`S`.`T`"));
  }

  @Test
  void testInsertWithTargetFieldsOnProject() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("AGE", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus testSchema = rootSchema.add("TEST", new AbstractSchema());
    testSchema.add("SRC", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
    // Build a project that selects ID and NAME (simulating a projection)
    RelNode project = builder.scan("TEST", "SRC")
        .project(
            builder.field("ID"),
            builder.field("NAME"))
        .build();

    ImmutablePairList<Integer, String> targetFields = ImmutablePairList.copyOf(Arrays.asList(
        new AbstractMap.SimpleEntry<>(0, "ID"),
        new AbstractMap.SimpleEntry<>(1, "NAME")));

    String sql = ScriptImplementor.empty()
        .insert(null, "S", "T", null, project, targetFields)
        .sql();

    assertTrue(sql.contains("INSERT INTO"));
    assertTrue(sql.contains("`S`.`T`"));
  }

  @Test
  void testInsertWithTargetFieldsOnTableScan() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("AGE", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus testSchema = rootSchema.add("TEST", new AbstractSchema());
    testSchema.add("SRC", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
    // Use a table scan (non-Project) to exercise the index-based path in dropFields
    RelNode scan = builder.scan("TEST", "SRC").build();

    ImmutablePairList<Integer, String> targetFields = ImmutablePairList.copyOf(Arrays.asList(
        new AbstractMap.SimpleEntry<>(0, "ID"),
        new AbstractMap.SimpleEntry<>(1, "NAME")));

    String sql = ScriptImplementor.empty()
        .insert(null, "S", "T", null, scan, targetFields)
        .sql();

    assertTrue(sql.contains("INSERT INTO"));
    assertTrue(sql.contains("`S`.`T`"));
    // Should use targetFields right list for column names (non-Project path)
    assertTrue(sql.contains("ID"));
    assertTrue(sql.contains("NAME"));
  }

  @Test
  void testInsertWithSuffix() {
    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(Frameworks.createRootSchema(true))
            .build());
    RelNode values = builder.values(new String[]{"COL1"}, "val1").build();

    String sql = ScriptImplementor.empty()
        .insert(null, "S", "T", "_sink", values, null)
        .sql();

    assertTrue(sql.contains("INSERT INTO"));
    assertTrue(sql.contains("T_sink"));
  }

  @Test
  void testInsertWithTableNameReplacements() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("COL1", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus testSchema = rootSchema.add("TEST", new AbstractSchema());
    testSchema.add("MY_TABLE", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
    RelNode scan = builder.scan("TEST", "MY_TABLE").build();

    Map<String, String> replacements = new HashMap<>();
    replacements.put("TEST.MY_TABLE", "MY_TABLE_source");

    String sql = ScriptImplementor.empty()
        .insert(null, "S", "T", null, scan, null, replacements)
        .sql();

    assertTrue(sql.contains("INSERT INTO"));
    assertTrue(sql.contains("MY_TABLE_source"));
  }

  @Test
  void testColumnListImplementorWithDollarSigns() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("FOO$BAR", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    String sql = ScriptImplementor.empty()
        .connector(null, "S", "T", rowType, Collections.emptyMap())
        .sql();

    // $-separated names are converted to underscores
    assertTrue(sql.contains("FOO_BAR"));
  }
}
