package com.linkedin.hoptimator.venice;

import java.util.Collections;

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

import com.linkedin.venice.client.schema.StoreSchemaFetcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        java.util.Map.of(VeniceStoreConfig.KEY_VALUE_SCHEMA_ID, "5"));
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
}
