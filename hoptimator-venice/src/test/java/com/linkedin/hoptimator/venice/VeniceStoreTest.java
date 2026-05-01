package com.linkedin.hoptimator.venice;

import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class VeniceStoreTest {

  @Mock
  private StoreSchemaFetcher mockSchemaFetcher;

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  @Test
  void testGetRowTypeWithStructKeyAndValueSchema() {
    Schema keySchema = SchemaBuilder.record("Key").fields()
        .requiredString("id")
        .endRecord();
    Schema valueSchema = SchemaBuilder.record("Value").fields()
        .requiredString("name")
        .requiredInt("age")
        .endRecord();

    when(mockSchemaFetcher.getKeySchema()).thenReturn(keySchema);
    when(mockSchemaFetcher.getLatestValueSchema()).thenReturn(valueSchema);

    VeniceStoreConfig config = new VeniceStoreConfig(Collections.emptyMap());
    VeniceStore store = new VeniceStore(mockSchemaFetcher, config);
    RelDataType rowType = store.getRowType(typeFactory);

    assertNotNull(rowType);
    // Key field should have KEY_ prefix
    assertTrue(rowType.getFieldNames().contains("KEY_id"));
    // Value fields should be present
    assertTrue(rowType.getFieldNames().contains("name"));
    assertTrue(rowType.getFieldNames().contains("age"));
  }

  @Test
  void testGetRowTypeWithPrimitiveKeySchema() {
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = SchemaBuilder.record("Value").fields()
        .requiredString("data")
        .endRecord();

    when(mockSchemaFetcher.getKeySchema()).thenReturn(keySchema);
    when(mockSchemaFetcher.getLatestValueSchema()).thenReturn(valueSchema);

    VeniceStoreConfig config = new VeniceStoreConfig(Collections.emptyMap());
    VeniceStore store = new VeniceStore(mockSchemaFetcher, config);
    RelDataType rowType = store.getRowType(typeFactory);

    assertNotNull(rowType);
    // Primitive key gets "KEY" name without prefix
    assertTrue(rowType.getFieldNames().contains("KEY"));
    assertTrue(rowType.getFieldNames().contains("data"));
  }

  @Test
  void testGetRowTypeUsesSpecificValueSchemaIdWhenConfigured() {
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = SchemaBuilder.record("Value").fields()
        .requiredString("field1")
        .endRecord();

    when(mockSchemaFetcher.getKeySchema()).thenReturn(keySchema);
    when(mockSchemaFetcher.getValueSchema(5)).thenReturn(valueSchema);

    VeniceStoreConfig config = new VeniceStoreConfig(
        Map.of(VeniceStoreConfig.KEY_VALUE_SCHEMA_ID, "5"));
    VeniceStore store = new VeniceStore(mockSchemaFetcher, config);
    RelDataType rowType = store.getRowType(typeFactory);

    assertNotNull(rowType);
    verify(mockSchemaFetcher).getValueSchema(5);
  }

  @Test
  void testGetRowTypeUsesLatestValueSchemaWhenNoIdConfigured() {
    Schema keySchema = Schema.create(Schema.Type.STRING);
    Schema valueSchema = SchemaBuilder.record("Value").fields()
        .requiredString("field1")
        .endRecord();

    when(mockSchemaFetcher.getKeySchema()).thenReturn(keySchema);
    when(mockSchemaFetcher.getLatestValueSchema()).thenReturn(valueSchema);

    VeniceStoreConfig config = new VeniceStoreConfig(Collections.emptyMap());
    VeniceStore store = new VeniceStore(mockSchemaFetcher, config);
    RelDataType rowType = store.getRowType(typeFactory);

    assertNotNull(rowType);
    verify(mockSchemaFetcher).getLatestValueSchema();
  }

  @Test
  void testGetRowTypeFieldCount() {
    Schema keySchema = SchemaBuilder.record("Key").fields()
        .requiredString("k1")
        .requiredString("k2")
        .endRecord();
    Schema valueSchema = SchemaBuilder.record("Value").fields()
        .requiredString("v1")
        .endRecord();

    when(mockSchemaFetcher.getKeySchema()).thenReturn(keySchema);
    when(mockSchemaFetcher.getLatestValueSchema()).thenReturn(valueSchema);

    VeniceStoreConfig config = new VeniceStoreConfig(Collections.emptyMap());
    VeniceStore store = new VeniceStore(mockSchemaFetcher, config);
    RelDataType rowType = store.getRowType(typeFactory);

    // 2 key fields (KEY_k1, KEY_k2) + 1 value field (v1)
    assertEquals(3, rowType.getFieldCount());
  }

  // --- valueSchema / keySchema wiring tests. Merging logic is covered by AvroSchemasTest. ---

  @Test
  void valueSchemaReturnsLatestValueSchemaAsIs() {
    // valueSchema() returns the raw payload from the fetcher — no cloning, no KEY_ fields. This
    // is what connector payload options (e.g. Flink's default.mode.payload) render.
    Schema valueSchema = SchemaBuilder.record("User").namespace("com.linkedin.foo").fields()
        .requiredString("name").requiredInt("age").endRecord();

    when(mockSchemaFetcher.getLatestValueSchema()).thenReturn(valueSchema);

    VeniceStore store = new VeniceStore(mockSchemaFetcher,
        new VeniceStoreConfig(Collections.emptyMap()));

    assertSame(valueSchema, store.valueSchema(), "valueSchema() returns the raw value schema");
  }

  @Test
  void keySchemaReturnsKeySchemaAsIs() {
    Schema keySchema = SchemaBuilder.record("Key").namespace("com.linkedin.keyns").fields()
        .requiredString("id").endRecord();

    when(mockSchemaFetcher.getKeySchema()).thenReturn(keySchema);

    VeniceStore store = new VeniceStore(mockSchemaFetcher,
        new VeniceStoreConfig(Collections.emptyMap()));

    assertSame(keySchema, store.keySchema(), "keySchema() returns the raw key schema");
  }

  @Test
  void keySchemaReturnsPrimitiveKeyAsPrimitive() {
    // Primitive keys come back as primitive Schemas (not wrapped in a synthetic record) so the
    // merge helper can apply its primitive-key logic (single "KEY" field).
    Schema keySchema = Schema.create(Schema.Type.STRING);

    when(mockSchemaFetcher.getKeySchema()).thenReturn(keySchema);

    VeniceStore store = new VeniceStore(mockSchemaFetcher,
        new VeniceStoreConfig(Collections.emptyMap()));

    Schema result = store.keySchema();
    assertSame(keySchema, result);
    assertEquals(Schema.Type.STRING, result.getType());
  }

  @Test
  void valueSchemaUsesConfiguredValueSchemaId() {
    Schema valueSchema = SchemaBuilder.record("User").namespace("com.linkedin.foo").fields()
        .requiredString("name").endRecord();

    when(mockSchemaFetcher.getValueSchema(7)).thenReturn(valueSchema);

    VeniceStoreConfig config = new VeniceStoreConfig(Map.of(VeniceStoreConfig.KEY_VALUE_SCHEMA_ID, "7"));
    VeniceStore store = new VeniceStore(mockSchemaFetcher, config);

    assertSame(valueSchema, store.valueSchema());
    verify(mockSchemaFetcher).getValueSchema(7);
  }

  @Test
  void mergedAvroSchemaHelperCombinesKeyAndValue() {
    // End-to-end: AvroSchemas.mergedAvroSchemaFor on a VeniceStore yields the KEY_-prefixed view.
    Schema keySchema = SchemaBuilder.record("Key").namespace("com.linkedin.keyns").fields()
        .requiredString("id").endRecord();
    Schema valueSchema = SchemaBuilder.record("User").namespace("com.linkedin.foo").fields()
        .requiredString("name").endRecord();

    when(mockSchemaFetcher.getKeySchema()).thenReturn(keySchema);
    when(mockSchemaFetcher.getLatestValueSchema()).thenReturn(valueSchema);

    VeniceStore store = new VeniceStore(mockSchemaFetcher,
        new VeniceStoreConfig(Collections.emptyMap()));

    Schema merged = com.linkedin.hoptimator.avro.AvroSchemas.mergedAvroSchemaFor(store);

    assertEquals("com.linkedin.foo", merged.getNamespace());
    assertEquals("User", merged.getName());
    assertEquals(2, merged.getFields().size());
    assertEquals("KEY_id", merged.getFields().get(0).name());
    assertEquals("name", merged.getFields().get(1).name());
  }
}
