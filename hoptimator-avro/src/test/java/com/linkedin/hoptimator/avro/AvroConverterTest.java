package com.linkedin.hoptimator.avro;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class AvroConverterTest {

  @Test
  public void convertsNestedSchemas() {
    String schemaString =
        "{\"type\":\"record\",\"name\":\"E\",\"namespace\":\"ns\",\"fields\":["
            + "{\"name\":\"h\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"H\",\"namespace\":\"ns\",\"fields\":["
            + "{\"name\":\"A\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"A\",\"fields\":[]}]}]}]}]}";

    Schema avroSchema1 = (new Schema.Parser()).parse(schemaString);
    RelDataType rel1 = AvroConverter.rel(avroSchema1);
    assertEquals(rel1.getFieldCount(), avroSchema1.getFields().size());
    assertNotNull(rel1.getField("h", false, false));
    RelDataType rel2 = Objects.requireNonNull(rel1.getField("h", false, false)).getType();
    assertTrue(rel2.isNullable());
    Schema avroSchema2 = avroSchema1.getField("h").schema().getTypes().get(1);
    assertEquals(rel2.getFieldCount(), avroSchema2.getFields().size());
    assertNotNull(rel2.getField("A", false, false));
    RelDataType rel3 = Objects.requireNonNull(rel2.getField("A", false, false)).getType();
    assertTrue(rel3.isNullable());
    Schema avroSchema3 = avroSchema2.getField("A").schema().getTypes().get(1);
    assertEquals(rel3.getFieldCount(), avroSchema3.getFields().size());
    Schema avroSchema4 = AvroConverter.avro("NS", "R", rel1);
    assertFalse(avroSchema4.isNullable());
    assertEquals(avroSchema4.getFields().size(), rel1.getFieldCount());
    Schema avroSchema5 = AvroConverter.avro("NS", "R", rel2);
    assertTrue(avroSchema5.isNullable());
    assertEquals(avroSchema5.getTypes().get(1).getFields().size(), rel2.getFieldCount());
    Schema avroSchema6 = AvroConverter.avro("NS", "R", rel3);
    assertEquals(avroSchema6.getTypes().get(1).getFields().size(), rel3.getFieldCount());
    RelDataType rel4 = AvroConverter.rel(avroSchema4);
    assertTrue(RelOptUtil.eq("rel4", rel4, "rel1", rel1, Litmus.THROW));
  }

  @Test
  public void convertsNestedUnionSchemas() {
    String schemaString =
        "{\"type\":\"record\",\"name\":\"record\",\"namespace\":\"ns\",\"fields\":["
            + "{\"name\":\"event\",\"type\":[{\"type\":\"record\",\"name\":\"record_event1\",\"fields\":["
            + "{\"name\":\"strField\",\"type\":\"string\"}]},{\"type\":\"record\",\"name\":\"record_event2\",\"fields\":["
            + "{\"name\":\"strField\",\"type\":\"string\"}]}]}]}";

    Schema avroSchema1 = (new Schema.Parser()).parse(schemaString);
    RelDataType rel1 = AvroConverter.rel(avroSchema1);
    assertEquals(rel1.getFieldCount(), avroSchema1.getFields().size());
    assertNotNull(rel1.getField("event", false, false));
    RelDataType rel2 = Objects.requireNonNull(rel1.getField("event", false, false)).getType();
    assertTrue(rel2.isStruct());
    Schema avroSchema2 = avroSchema1.getField("event").schema();
    assertEquals(rel2.getFieldCount(), avroSchema2.getTypes().size());
    RelDataType rel3 = Objects.requireNonNull(rel2.getField("record_event1", false, false)).getType();
    Schema avroSchema3 = avroSchema2.getTypes().get(0);
    assertEquals(rel3.getFieldCount(), avroSchema3.getFields().size());
    Schema avroSchema4 = AvroConverter.avro("NS", "R", rel1);
    assertFalse(avroSchema4.isNullable());
    assertEquals(avroSchema4.getFields().size(), rel1.getFieldCount());
  }

  @Test
  public void supportsNullTypes() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rel = typeFactory.createStructType(Collections.singletonList(typeFactory.createSqlType(SqlTypeName.NULL)),
        Collections.singletonList("field1"));

    Schema avroSchema = AvroConverter.avro("NS", "R", rel);
    assertEquals(avroSchema.getFields().size(), rel.getFieldCount());
  }

  @Test
  public void testAvroKeyPayloadSchemaNoKeyOptions() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(Collections.singletonList(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
        Collections.singletonList("field1"));

    Map<String, String> keyOptions = Map.of(); // No key options provided
    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema("namespace", "keySchema", "payloadSchema", dataType, keyOptions);

    assertNull(result.getKey()); // Key schema should be null
    assertNotNull(result.getValue()); // Payload schema should not be null
    assertEquals("payloadSchema", result.getValue().getName());
    assertEquals("namespace.payloadSchema", result.getValue().getNamespace());
    assertEquals("record", result.getValue().getType().getName());
    assertEquals(1, result.getValue().getFields().size());
    assertEquals("field1", result.getValue().getFields().get(0).name());
    assertEquals("string", result.getValue().getFields().get(0).schema().getType().getName());
  }

  @Test
  public void testAvroKeyPayloadSchemaNonStructDataType() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createSqlType(SqlTypeName.VARCHAR); // Non-struct type

    Map<String, String> keyOptions = Map.of(); // No key options provided
    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema("namespace", "keySchema", "payloadSchema", dataType, keyOptions);

    assertNull(result.getKey()); // Key schema should be null
    assertNotNull(result.getValue()); // Payload schema should not be null
    assertEquals("string", result.getValue().getType().getName());
  }

  @Test
  public void testAvroKeyPayloadSchemaValidKeyOptions() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.INTEGER)),
        List.of("KEY_field1", "field2"));

    Map<String, String> keyOptions = Map.of(
        "key.fields", "KEY_field1",
        "key.fields-prefix", "KEY_"
    );
    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema("namespace", "keySchema", "payloadSchema", dataType, keyOptions);

    assertNotNull(result.getKey()); // Key schema should not be null
    assertEquals("keySchema", result.getKey().getName());
    assertEquals("namespace.keySchema", result.getKey().getNamespace());
    assertEquals("record", result.getKey().getType().getName());
    assertEquals(1, result.getKey().getFields().size());
    assertEquals("field1", result.getKey().getFields().get(0).name()); // prefix should be stripped
    assertEquals("string", result.getKey().getFields().get(0).schema().getType().getName());
    assertNotNull(result.getValue()); // Payload schema should not be null
    assertEquals("payloadSchema", result.getValue().getName());
    assertEquals("namespace.payloadSchema", result.getValue().getNamespace());
    assertEquals("record", result.getValue().getType().getName());
    assertEquals(1, result.getValue().getFields().size());
    assertEquals("field2", result.getValue().getFields().get(0).name());
    assertEquals("int", result.getValue().getFields().get(0).schema().getType().getName());
  }

  @Test
  public void testAvroKeyPayloadSchemaPrimitiveKey() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.INTEGER)),
        List.of("field1", "KEY"));

    Map<String, String> keyOptions = Map.of(
        "key.fields", "KEY"
    );
    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema("namespace", "keySchema", "payloadSchema", dataType, keyOptions);

    assertNotNull(result.getKey()); // Key schema should not be null
    assertEquals("int", result.getKey().getType().getName());
    assertNotNull(result.getValue()); // Payload schema should not be null
    assertEquals("payloadSchema", result.getValue().getName());
    assertEquals("namespace.payloadSchema", result.getValue().getNamespace());
    assertEquals("record", result.getValue().getType().getName());
    assertEquals(1, result.getValue().getFields().size());
    assertEquals("field1", result.getValue().getFields().get(0).name());
    assertEquals("string", result.getValue().getFields().get(0).schema().getType().getName());
  }

  @Test
  public void convertsNestedArray() {
    // Create a RelDataType with an array of structs
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // Create a struct type for array elements
    RelDataTypeFactory.Builder elementBuilder = new RelDataTypeFactory.Builder(typeFactory);
    elementBuilder.add("field1", typeFactory.createSqlType(SqlTypeName.VARCHAR));
    elementBuilder.add("field2", typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataType structType = elementBuilder.build();

    // Create array of structs type
    RelDataType arrayOfStructsType = typeFactory.createArrayType(structType, -1);

    // Test with a struct containing an array of structs field
    RelDataTypeFactory.Builder containerBuilder = new RelDataTypeFactory.Builder(typeFactory);
    containerBuilder.add("arrayOfStructsField", arrayOfStructsType);
    RelDataType containerType = containerBuilder.build();

    Schema containerSchema = AvroConverter.avro("test", "Record", containerType);
    assertNotNull(containerSchema);
    assertEquals(1, containerSchema.getFields().size());
    assertEquals("arrayOfStructsField", containerSchema.getFields().get(0).name());

    Schema arrayFieldSchema = containerSchema.getFields().get(0).schema();
    assertEquals(Schema.Type.ARRAY, arrayFieldSchema.getType());

    Schema structElementSchema = arrayFieldSchema.getElementType();
    assertEquals(Schema.Type.RECORD, structElementSchema.getType());
    assertEquals(2, structElementSchema.getFields().size());
    assertEquals("field1", structElementSchema.getFields().get(0).name());
    assertEquals("field2", structElementSchema.getFields().get(1).name());
  }

  @Test
  public void handlesNamespaceInNestedArrayAndMapElements() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // Create a "location" record type that will be reused - this mimics the real scenario
    RelDataType locationType1 = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.VARCHAR)),
        List.of("countryCode", "postalCode"));

    // Create another "location" record type with slightly different structure
    RelDataType locationType2 = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.INTEGER)),
        List.of("countryCode", "regionCode"));

    // Create structures that use these location types in different contexts
    // This simulates the real scenario where multiple fields have the same name but different contexts
    RelDataType profileStruct = typeFactory.createStructType(
        List.of(locationType1),
        List.of("location"));

    RelDataType positionStruct = typeFactory.createStructType(
        List.of(locationType2),
        List.of("location"));

    // Put both in a map structure - this creates the collision scenario
    // Both will try to generate records named "location" with the same namespace
    RelDataType positionsMap = typeFactory.createMapType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        positionStruct);

    // Create the main record that contains both location types
    RelDataType mainRecord = typeFactory.createStructType(
        List.of(profileStruct, positionsMap),
        List.of("profile", "positions"));

    // Schema creation should succeed
    Schema schema = AvroConverter.avro("com.linkedin", "MemberProfile", mainRecord);
    assertNotNull(schema);

    // Without the namespace-appending behavior in AvroConverter, this would fail with error "Can't redefine: com.linkedin.location"
    // The issue occurs because multiple records named "location" are created with the same namespace,
    // causing a collision when schema.toString(true) tries to serialize them
    String schemaJson = schema.toString(true);
    assertNotNull("Schema toString(true) should succeed without 'Can't redefine' errors", schemaJson);

    // Verify the schema can be parsed back
    Schema.Parser parser = new Schema.Parser();
    Schema reparsedSchema = parser.parse(schemaJson);
    assertNotNull(reparsedSchema);
  }
}
