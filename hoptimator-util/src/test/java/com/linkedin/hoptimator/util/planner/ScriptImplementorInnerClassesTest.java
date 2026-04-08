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
import static org.junit.jupiter.api.Assertions.assertFalse;
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

  @Test
  void testDropFieldsOnProjectRemovesExtraFields() {
    // Source has [A, B, C]; target is only [A, B] — C must be absent from the INSERT
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("A", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("B", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("C", typeFactory.createSqlType(SqlTypeName.BOOLEAN))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus testSchema = rootSchema.add("T", new AbstractSchema());
    testSchema.add("SRC", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder().defaultSchema(rootSchema).build());
    // Project selects all three fields explicitly — matches the "relNode instanceof Project" path
    RelNode project = builder.scan("T", "SRC")
        .project(builder.field("A"), builder.field("B"), builder.field("C"))
        .build();

    // Target only wants A and B
    ImmutablePairList<Integer, String> targetFields = ImmutablePairList.copyOf(Arrays.asList(
        new AbstractMap.SimpleEntry<>(0, "A"),
        new AbstractMap.SimpleEntry<>(1, "B")));

    String sql = ScriptImplementor.empty()
        .insert(null, "S", "DEST", null, project, targetFields)
        .sql();

    assertTrue(sql.contains("`A`"), "Should contain field A. Got: " + sql);
    assertTrue(sql.contains("`B`"), "Should contain field B. Got: " + sql);
    assertFalse(sql.contains("`C`"), "Should NOT contain field C. Got: " + sql);
  }

  @Test
  void testDropFieldsOnTableScanRemovesExtraFieldsByIndex() {
    // Non-Project (TableScan) path: index-based matching — source has [ID, NAME, AGE],
    // target only wants [ID, NAME] by index; AGE must be absent from the SELECT.
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("ID",   typeFactory.createSqlType(SqlTypeName.INTEGER))
        .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("AGE",  typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus testSchema = rootSchema.add("S2", new AbstractSchema());
    testSchema.add("SRC2", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder().defaultSchema(rootSchema).build());
    // Raw scan — exercises the non-Project (index-based) branch
    RelNode scan = builder.scan("S2", "SRC2").build();

    ImmutablePairList<Integer, String> targetFields = ImmutablePairList.copyOf(Arrays.asList(
        new AbstractMap.SimpleEntry<>(0, "ID"),
        new AbstractMap.SimpleEntry<>(1, "NAME")));

    String sql = ScriptImplementor.empty()
        .insert(null, "DEST", "T2", null, scan, targetFields)
        .sql();

    assertTrue(sql.contains("`ID`"), "Should contain ID. Got: " + sql);
    assertTrue(sql.contains("`NAME`"), "Should contain NAME. Got: " + sql);
    assertFalse(sql.contains("`AGE`"), "Should NOT contain AGE. Got: " + sql);
  }

  @Test
  void testDropNullFieldsRemovesNullTypedColumns() {
    // A NULL-typed column must be dropped; a VARCHAR column must be retained.
    // Use a schema-based table with an explicit NULL-typed column.
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("GOOD_COL", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("NULL_COL", typeFactory.createSqlType(SqlTypeName.NULL))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus testSchema = rootSchema.add("NULL_SCH", new AbstractSchema());
    testSchema.add("NULL_TBL", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder().defaultSchema(rootSchema).build());
    RelNode scan = builder.scan("NULL_SCH", "NULL_TBL").build();

    // insert() with null targetFields triggers dropNullFields()
    String sql = ScriptImplementor.empty()
        .insert(null, "S", "T", scan)
        .sql();

    assertTrue(sql.contains("`GOOD_COL`"), "Should retain non-null column. Got: " + sql);
    assertFalse(sql.contains("`NULL_COL`"), "Should drop NULL-typed column. Got: " + sql);
  }

  @Test
  void testDropNullFieldsRetainsAllNonNullColumns() {
    // When no NULL-typed columns exist, all columns should be present
    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(Frameworks.createRootSchema(true))
            .build());
    RelNode values = builder.values(new String[]{"COL_A", "COL_B"}, "x", "y").build();

    String sql = ScriptImplementor.empty()
        .insert(null, "S", "T", values)
        .sql();

    assertTrue(sql.contains("`COL_A`"), "Should retain COL_A. Got: " + sql);
    assertTrue(sql.contains("`COL_B`"), "Should retain COL_B. Got: " + sql);
  }

  // The ANSI SQL unparser does not emit "NOT NULL" in the column spec text,
  // so we verify the behavior indirectly: both nullable and non-nullable
  // columns appear correctly in the CREATE TABLE output, and crucially
  // the SQL does NOT incorrectly include a "NULL" keyword for either case.

  @Test
  void testNonNullableColumnAppearsInCreateTable() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType notNullVarchar = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), false);
    RelDataType rowType = typeFactory.createStructType(
        Collections.singletonList(notNullVarchar),
        Collections.singletonList("STRICT_COL"));

    String sql = ScriptImplementor.empty()
        .connector(null, "S", "T", rowType, Collections.emptyMap())
        .sql();

    // Column must be present in the CREATE TABLE DDL
    assertTrue(sql.contains("STRICT_COL"), "Non-nullable column must appear in DDL. Got: " + sql);
    assertTrue(sql.contains("VARCHAR"), "Non-nullable column type must appear in DDL. Got: " + sql);
    // NULL keyword must not appear — the code comment says "we don't want VARCHAR NULL"
    assertFalse(sql.contains("VARCHAR NULL"),
        "Non-nullable column must not have VARCHAR NULL. Got: " + sql);
  }

  @Test
  void testNullableColumnAppearsInCreateTableWithoutNullKeyword() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    // createSqlType produces nullable by default
    RelDataType nullableVarchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType rowType = typeFactory.createStructType(
        Collections.singletonList(nullableVarchar),
        Collections.singletonList("NULLABLE_COL"));

    String sql = ScriptImplementor.empty()
        .connector(null, "S", "T", rowType, Collections.emptyMap())
        .sql();

    assertTrue(sql.contains("NULLABLE_COL"), "Nullable column must appear in DDL. Got: " + sql);
    assertTrue(sql.contains("VARCHAR"), "Nullable column type must appear in DDL. Got: " + sql);
    // The spec comment: "we don't want 'VARCHAR NULL', only 'VARCHAR NOT NULL'"
    assertFalse(sql.contains("VARCHAR NULL"),
        "Nullable column must not produce VARCHAR NULL. Got: " + sql);
  }

  // When relNode is a Project AND targetFields != null → column list comes from
  // project.getRowType().getFieldNames(), not targetFields.rightList().
  @Test
  void testInsertColumnListComesFromProjectFieldNamesWhenProjectWithTargetFields() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType tableType = typeFactory.builder()
        .add("SRC_A", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("SRC_B", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus testSchema = rootSchema.add("SRC_SCH", new AbstractSchema());
    testSchema.add("SRC_TBL", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory tf) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder().defaultSchema(rootSchema).build());
    // Project renames fields — the column list in INSERT should use projected names
    RelNode project = builder.scan("SRC_SCH", "SRC_TBL")
        .project(
            builder.alias(builder.field("SRC_A"), "RENAMED_A"),
            builder.alias(builder.field("SRC_B"), "RENAMED_B"))
        .build();

    ImmutablePairList<Integer, String> targetFields = ImmutablePairList.copyOf(Arrays.asList(
        new AbstractMap.SimpleEntry<>(0, "RENAMED_A"),
        new AbstractMap.SimpleEntry<>(1, "RENAMED_B")));

    String sql = ScriptImplementor.empty()
        .insert(null, "DEST_SCH", "DEST_TBL", null, project, targetFields)
        .sql();

    assertTrue(sql.contains("INSERT INTO"), "Should produce INSERT INTO. Got: " + sql);
    // Column list should reference the projected (renamed) field names
    assertTrue(sql.contains("RENAMED_A"), "Column list should contain RENAMED_A. Got: " + sql);
    assertTrue(sql.contains("RENAMED_B"), "Column list should contain RENAMED_B. Got: " + sql);
  }
}
