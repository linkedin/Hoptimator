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

/**
 * Tests for ScriptImplementor
 */
public class ScriptImplementorTest {
  @Test
  public void testConnectorWithSuffix() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("CAMPAIGN_URN", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("MEMBER_URN", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    Map<String, String> config = new HashMap<>();
    config.put("connector", "datagen");
    config.put("number-of-rows", "10");

    String sql = ScriptImplementor.empty()
        .connector(null, "ADS", "AD_CLICKS", "_source", rowType, config)
        .sql();

    assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS `ADS`.`AD_CLICKS_source`"),
        "Should create table with _source suffix. Got: " + sql);
    assertTrue(sql.contains("'connector'='datagen'"),
        "Should include connector config. Got: " + sql);
  }

  @Test
  public void testInsertWithSuffix() {
    // Create a simple RelNode for testing
    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(Frameworks.createRootSchema(true))
            .build());

    RelNode scan = builder
        .values(new String[]{"CAMPAIGN_URN", "MEMBER_URN"}, "urn1", "urn2")
        .build();

    ImmutablePairList<Integer, String> targetFields = ImmutablePairList.copyOf(Arrays.asList(
        new AbstractMap.SimpleEntry<>(0, "CAMPAIGN_URN"),
        new AbstractMap.SimpleEntry<>(1, "MEMBER_URN")
    ));

    String sql = ScriptImplementor.empty()
        .insert(null, "ADS", "AD_CLICKS", "_sink", scan, targetFields)
        .sql();

    assertTrue(sql.contains("INSERT INTO `ADS`.`AD_CLICKS_sink`"),
        "Should insert into table with _sink suffix. Got: " + sql);
  }

  @Test
  public void testTableNameReplacements() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // Create a schema with a table to scan from
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus adsSchema = rootSchema.add("ADS", new AbstractSchema());

    // Add a mock table
    RelDataType tableType = typeFactory.builder()
        .add("CAMPAIGN_URN", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("MEMBER_URN", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    adsSchema.add("AD_CLICKS", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return tableType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());

    // Create a scan that references ADS.AD_CLICKS
    RelNode scan = builder
        .scan("ADS", "AD_CLICKS")
        .build();

    ImmutablePairList<Integer, String> targetFields = ImmutablePairList.copyOf(Arrays.asList(
        new AbstractMap.SimpleEntry<>(0, "CAMPAIGN_URN"),
        new AbstractMap.SimpleEntry<>(1, "MEMBER_URN")
    ));

    // Test that table name replacement works in the SELECT query
    Map<String, String> tableReplacements = new HashMap<>();
    tableReplacements.put("ADS.AD_CLICKS", "AD_CLICKS_source");

    String sql = ScriptImplementor.empty()
        .insert(null, "ADS", "AD_CLICKS", "_sink", scan, targetFields, tableReplacements)
        .sql();

    assertTrue(sql.contains("INSERT INTO `ADS`.`AD_CLICKS_sink`"),
        "Should insert into table with _sink suffix. Got: " + sql);
    assertTrue(sql.contains("FROM `ADS`.`AD_CLICKS_source`"),
        "Should select from table with _source suffix (table name replaced). Got: " + sql);
  }

  @Test
  public void testFullPipelineWithCollision() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("CAMPAIGN_URN", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("MEMBER_URN", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    Map<String, String> sourceConfig = new HashMap<>();
    sourceConfig.put("connector", "datagen");
    sourceConfig.put("number-of-rows", "10");

    Map<String, String> sinkConfig = new HashMap<>();
    sinkConfig.put("connector", "blackhole");

    // Create a simple RelNode
    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(Frameworks.createRootSchema(true))
            .build());

    RelNode scan = builder
        .values(new String[]{"CAMPAIGN_URN", "MEMBER_URN"}, "urn1", "urn2")
        .build();

    ImmutablePairList<Integer, String> targetFields = ImmutablePairList.copyOf(Arrays.asList(
        new AbstractMap.SimpleEntry<>(0, "CAMPAIGN_URN"),
        new AbstractMap.SimpleEntry<>(1, "MEMBER_URN")
    ));

    String sql = ScriptImplementor.empty()
        .database(null, "ADS")
        .connector(null, "ADS", "AD_CLICKS", "_source", rowType, sourceConfig)
        .database(null, "ADS")
        .connector(null, "ADS", "AD_CLICKS", "_sink", rowType, sinkConfig)
        .insert(null, "ADS", "AD_CLICKS", "_sink", scan, targetFields)
        .sql();

    // Verify both tables are created with different suffixes
    assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS `ADS`.`AD_CLICKS_source`"),
        "Should create source table with suffix. Got: " + sql);
    assertTrue(sql.contains("CREATE TABLE IF NOT EXISTS `ADS`.`AD_CLICKS_sink`"),
        "Should create sink table with suffix. Got: " + sql);
    assertTrue(sql.contains("'connector'='datagen'"),
        "Should include datagen connector. Got: " + sql);
    assertTrue(sql.contains("'connector'='blackhole'"),
        "Should include blackhole connector. Got: " + sql);
    assertTrue(sql.contains("INSERT INTO `ADS`.`AD_CLICKS_sink`"),
        "Should insert into sink table. Got: " + sql);
  }

  @Test
  public void testFunctionWithoutLanguage() {
    String sql = ScriptImplementor.empty()
        .function("my_udf", "com.example.MyUdf", Collections.emptyMap())
        .sql();

    assertTrue(sql.contains("CREATE FUNCTION IF NOT EXISTS"),
        "Should contain CREATE FUNCTION IF NOT EXISTS. Got: " + sql);
    assertTrue(sql.contains("my_udf") || sql.contains("`my_udf`"),
        "Should contain function name. Got: " + sql);
    assertTrue(sql.contains("'com.example.MyUdf'"),
        "Should contain AS clause. Got: " + sql);
    assertFalse(sql.contains("LANGUAGE"),
        "Should not contain LANGUAGE clause when not specified. Got: " + sql);
  }

  @Test
  public void testFunctionWithLanguage() {
    Map<String, String> options = new HashMap<>();
    options.put("LANGUAGE", "PYTHON");

    String sql = ScriptImplementor.empty()
        .function("my_udf", "my_module.my_func", options)
        .sql();

    assertTrue(sql.contains("CREATE FUNCTION IF NOT EXISTS"),
        "Should contain CREATE FUNCTION IF NOT EXISTS. Got: " + sql);
    assertTrue(sql.contains("'my_module.my_func'"),
        "Should contain AS clause. Got: " + sql);
    assertTrue(sql.contains("LANGUAGE"),
        "Should contain LANGUAGE clause. Got: " + sql);
    assertTrue(sql.contains("PYTHON"),
        "Should contain PYTHON language. Got: " + sql);
  }

  @Test
  public void testFunctionBeforeConnector() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder()
        .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    Map<String, String> config = new HashMap<>();
    config.put("connector", "datagen");

    Map<String, String> funcOptions = new HashMap<>();
    funcOptions.put("LANGUAGE", "PYTHON");

    String sql = ScriptImplementor.empty()
        .function("my_udf", "my_module.my_func", funcOptions)
        .database(null, "TEST")
        .connector(null, "TEST", "MY_TABLE", rowType, config)
        .sql();

    int funcIdx = sql.indexOf("CREATE FUNCTION");
    int tableIdx = sql.indexOf("CREATE TABLE");
    assertTrue(funcIdx >= 0, "Should contain CREATE FUNCTION. Got: " + sql);
    assertTrue(tableIdx >= 0, "Should contain CREATE TABLE. Got: " + sql);
    assertTrue(funcIdx < tableIdx,
        "CREATE FUNCTION should appear before CREATE TABLE. Got: " + sql);
  }

  @Test
  public void testExplicitColumnEnumeration() {
    // Test for Flink 1.20 regression where INSERT with SELECT * fails
    // when sink has more columns than source
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // Source table: 2 columns
    RelDataType sourceType = typeFactory.builder()
        .add("KEY_source", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("nestedValue_source", typeFactory.builder()
            .add("innerInt_source", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("innerArray_source", typeFactory.createArrayType(
                typeFactory.createSqlType(SqlTypeName.INTEGER), -1))
            .build())
        .build();

    // Create schema with source table
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus sourceSchema = rootSchema.add("source", new AbstractSchema());
    sourceSchema.add("table", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return sourceType;
      }
    });

    RelBuilder builder = RelBuilder.create(
        Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());

    // Create a query: SELECT KEY_source as KEY_sink, nestedValue_source as nestedValue_sink FROM source
    // This simulates the materialized view query
    RelNode query = builder
        .scan("source", "table")
        .project(
            builder.field("KEY_source"),
            builder.field("nestedValue_source"))
        .build();

    // Target fields for INSERT - only the 2 columns we're actually inserting
    ImmutablePairList<Integer, String> targetFields = ImmutablePairList.copyOf(Arrays.asList(
        new AbstractMap.SimpleEntry<>(0, "KEY_sink"),
        new AbstractMap.SimpleEntry<>(1, "nestedValue_sink")
    ));

    String sql = ScriptImplementor.empty()
        .insert(null, "sink", "mypipeline", null, query, targetFields)
        .sql();

    assertEquals(
        "INSERT INTO `sink`.`mypipeline` (`KEY_sink`, `nestedValue_sink`) "
            + "SELECT `KEY_source` AS `KEY_sink`, `nestedValue_source` AS `nestedValue_sink` "
            + "FROM `source`.`table`;", sql);
  }
}
