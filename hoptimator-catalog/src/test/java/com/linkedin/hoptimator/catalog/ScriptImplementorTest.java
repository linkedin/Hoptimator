package com.linkedin.hoptimator.catalog;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Test;

import com.linkedin.hoptimator.avro.AvroConverter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ScriptImplementorTest {

  @Test
  public void implementsFlinkCreateTableDDL() {
    String schemaString = "{\"type\" : \"record\", \"namespace\": \"foo\", \"name\": \"bar\","
        + "\"fields\" : [ {\"name\" : \"idValue1\",\"type\" : \"string\", \"doc\" : \"idValue1 VARCHAR NOT NULL\" } ]}";
    Schema avroSchema = (new Schema.Parser()).parse(schemaString);
    RelDataType rowType = AvroConverter.rel(avroSchema);
    ConfigProvider configProvider = ConfigProvider.empty()
        .with("connector", "kafka")
        .with("properties.bootstrap.servers", "localhost:9092")
        .with("topic", x -> x);
    HopTable table = new HopTable("DATABASE", "TABLE1", rowType, configProvider.config("topic1"));
    SqlWriter w = new SqlPrettyWriter();
    table.implement(w);
    String out = w.toString();
    // Output isn't necessarily deterministic, but should be something like:
    //   CREATE TABLE IF NOT EXISTS "DATABASE"."TABLE1" ("idValue1" VARCHAR) WITH
    //   ('connector'='kafka', 'properties.bootstrap.servers'='localhost:9092', 'topic'='topic1')
    assertTrue(out, out.contains("CREATE TABLE IF NOT EXISTS \"DATABASE\".\"TABLE1\" (\"idValue1\" VARCHAR) WITH "));
    assertTrue(out, out.contains("'connector'='kafka'"));
    assertTrue(out, out.contains("'properties.bootstrap.servers'='localhost:9092'"));
    assertTrue(out, out.contains("'topic'='topic1'"));
    assertFalse(out, out.contains("Row"));
  }

  @Test
  public void magicPrimaryKey() {
    SqlWriter w = new SqlPrettyWriter();
    RelDataType rowType = DataType.struct().with("F1", DataType.VARCHAR).with("PRIMARY_KEY", DataType.VARCHAR).rel();
    HopTable table = new HopTable("DATABASE", "TABLE1", rowType, ConfigProvider.empty().config("x"));
    table.implement(w);
    String out = w.toString();
    assertTrue(out, out.contains("PRIMARY KEY (PRIMARY_KEY)"));
  }

  @Test
  public void magicNullFields() {
    SqlWriter w = new SqlPrettyWriter();
    RelDataType rowType = DataType.struct().with("F1", DataType.VARCHAR).with("KEY", DataType.NULL).rel();
    HopTable table = new HopTable("DATABASE", "TABLE1", rowType, ConfigProvider.empty().config("x"));
    table.implement(w);
    String out = w.toString();
    assertTrue(out, out.contains("\"KEY\" BYTES")); // NULL fields are promoted to BYTES.
    assertFalse(out, out.contains("\"KEY\" NULL")); // Without magic, this is what you'd get.
  }
}
