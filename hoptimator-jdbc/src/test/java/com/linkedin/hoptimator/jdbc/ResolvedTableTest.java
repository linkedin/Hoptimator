package com.linkedin.hoptimator.jdbc;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class ResolvedTableTest {

  @Test
  void testSourceConnectorConfigs() {
    Map<String, String> sourceConfigs = Map.of("bootstrap.servers", "localhost:9092");
    Map<String, String> sinkConfigs = Collections.emptyMap();
    Schema avroSchema = Schema.create(Schema.Type.STRING);

    ResolvedTable table = new ResolvedTable(List.of("db", "table"), avroSchema, sourceConfigs, sinkConfigs);

    assertEquals(sourceConfigs, table.sourceConnectorConfigs());
  }

  @Test
  void testSinkConnectorConfigs() {
    Map<String, String> sourceConfigs = Collections.emptyMap();
    Map<String, String> sinkConfigs = Map.of("topic", "my-topic");
    Schema avroSchema = Schema.create(Schema.Type.STRING);

    ResolvedTable table = new ResolvedTable(List.of("db", "table"), avroSchema, sourceConfigs, sinkConfigs);

    assertEquals(sinkConfigs, table.sinkConnectorConfigs());
  }

  @Test
  void testAvroSchema() {
    Schema avroSchema = Schema.create(Schema.Type.INT);

    ResolvedTable table = new ResolvedTable(List.of("db", "table"), avroSchema,
        Collections.emptyMap(), Collections.emptyMap());

    assertEquals(avroSchema, table.avroSchema());
  }

  @Test
  void testAvroSchemaString() {
    Schema avroSchema = Schema.createRecord("TestRecord", null, "ns", false,
        List.of(new Schema.Field("field1", Schema.create(Schema.Type.STRING))));

    ResolvedTable table = new ResolvedTable(List.of("db", "table"), avroSchema,
        Collections.emptyMap(), Collections.emptyMap());

    String schemaString = table.avroSchemaString();
    assertNotNull(schemaString);
    assertTrue(schemaString.contains("TestRecord"));
    assertTrue(schemaString.contains("field1"));
  }
}
