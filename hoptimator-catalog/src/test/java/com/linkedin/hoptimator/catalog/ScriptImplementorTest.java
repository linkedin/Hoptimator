package com.linkedin.hoptimator.catalog;

import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.avro.Schema;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

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
    AdapterTable table = new AdapterTable("DATABASE", "TABLE1", rowType, configProvider.config("topic1"));
    SqlWriter w = new SqlPrettyWriter();
    table.implement(w);
    String out = w.toString();
    // Output isn't necessarily deterministic, but should be something like:
    //   CREATE TABLE "DATABASE"."TABLE1" ("idValue1" VARCHAR) WITH
    //   ('connector'='kafka', 'properties.bootstrap.servers'='localhost:9092', 'topic'='topic1')
    assertTrue(out.contains("CREATE TABLE \"DATABASE\".\"TABLE1\" (\"idValue1\" VARCHAR) WITH "));
    assertTrue(out.contains("'connector'='kafka'"));
    assertTrue(out.contains("'properties.bootstrap.servers'='localhost:9092'"));
    assertTrue(out.contains("'topic'='topic1'"));
    assertFalse(out.contains("Row")); 
  }
}
