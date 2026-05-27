package com.linkedin.hoptimator.avro;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    assertEquals(JsonProperties.NULL_VALUE, avroSchema4.getField("h").defaultVal());
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
    String schemaString =
        "{\"type\":\"record\",\"name\":\"record\",\"namespace\":\"ns\",\"fields\":["
            + "{\"name\":\"arrayOfStructsField\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"record_event1\",\"fields\":["
            + "{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"int\"}]}}}]}";
    Schema avroSchema = (new Schema.Parser()).parse(schemaString);
    RelDataType containerType = AvroConverter.rel(avroSchema); // Ensure parsing works
    // Calcite does not support inner array fields as NOT NULL.
    assertEquals(1, containerType.getFieldList().size());
    assertFalse(containerType.getFieldList().get(0).getType().isNullable());
    assertEquals(2, containerType.getFieldList().get(0).getType().getComponentType().getFieldList().size());
    assertTrue(containerType.getFieldList().get(0).getType().getComponentType().getFieldList().get(0).getType().isNullable());
    assertEquals("RecordType(RecordType(VARCHAR field1, INTEGER field2) ARRAY arrayOfStructsField) NOT NULL", containerType.getFullTypeString());

    Schema containerSchema = AvroConverter.avro("test", "Record", containerType);
    assertNotNull(containerSchema);
    assertEquals(1, containerSchema.getFields().size());
    assertEquals("arrayOfStructsField", containerSchema.getFields().get(0).name());

    Schema arrayFieldSchema = containerSchema.getFields().get(0).schema();
    assertEquals(Schema.Type.ARRAY, arrayFieldSchema.getType());

    Schema structElementSchema = arrayFieldSchema.getElementType();
    assertEquals(Schema.Type.UNION, structElementSchema.getType());
    assertEquals(2, structElementSchema.getTypes().size());
    assertEquals(Schema.Type.NULL, structElementSchema.getTypes().get(0).getType());
    assertEquals(Schema.Type.RECORD, structElementSchema.getTypes().get(1).getType());

    Schema innerRecord = structElementSchema.getTypes().get(1);
    assertEquals(2, innerRecord.getFields().size());
    assertEquals("field1", innerRecord.getFields().get(0).name());
    assertEquals("field2", innerRecord.getFields().get(1).name());
    assertEquals(2, innerRecord.getFields().size());

    Schema innermostField = innerRecord.getFields().get(0).schema();
    assertEquals(2, innermostField.getTypes().size());
    assertEquals(Schema.Type.NULL, innermostField.getTypes().get(0).getType());
    assertEquals(Schema.Type.STRING, innermostField.getTypes().get(1).getType());
  }

  @Test
  public void convertsNullableUnionFields() {
    // Schema with a nullable string field: ["null", "string"]
    String schemaString =
        "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"ns\",\"fields\":["
            + "{\"name\":\"nullableStr\",\"type\":[\"null\",\"string\"]},"
            + "{\"name\":\"nullableInt\",\"type\":[\"null\",\"int\"]},"
            + "{\"name\":\"requiredStr\",\"type\":\"string\"}]}";
    Schema avroSchema = (new Schema.Parser()).parse(schemaString);
    RelDataType rel = AvroConverter.rel(avroSchema);
    assertNotNull(rel);
    assertEquals(3, rel.getFieldCount());
    assertNotNull(rel.getField("nullableStr", false, false));
    assertTrue(Objects.requireNonNull(rel.getField("nullableStr", false, false)).getType().isNullable());
    assertNotNull(rel.getField("nullableInt", false, false));
    assertTrue(Objects.requireNonNull(rel.getField("nullableInt", false, false)).getType().isNullable());
    assertNotNull(rel.getField("requiredStr", false, false));
  }

  @Test
  public void convertsNullTypeField() {
    // Schema with a field whose type is just "null"
    String schemaString =
        "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"ns\",\"fields\":["
            + "{\"name\":\"nullField\",\"type\":\"null\"},"
            + "{\"name\":\"strField\",\"type\":\"string\"}]}";
    Schema avroSchema = (new Schema.Parser()).parse(schemaString);
    RelDataType rel = AvroConverter.rel(avroSchema);
    assertNotNull(rel);
    // The null-typed field should be filtered out (existing behavior) or handled gracefully
    assertNotNull(rel.getField("strField", false, false));
  }

  @Test
  public void convertsNullSchema() {
    // A standalone NULL schema should not throw NPE
    Schema nullSchema = Schema.create(Schema.Type.NULL);
    RelDataType rel = AvroConverter.rel(nullSchema);
    assertNotNull(rel);
  }

  @Test
  public void handlesNullSchemaParameter() {
    // Passing null schema should not throw NPE - should handle gracefully
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rel = AvroConverter.rel(null, typeFactory);
    assertNotNull(rel);
  }

  @Test
  public void sanitizeHandlesNameStartingWithNumber() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
        List.of("1"));
    Schema schema = AvroConverter.avro("ns", "Record", dataType);
    // sanitize("1") prepends underscore → "_1"
    assertEquals("_1", schema.getFields().get(0).name());
  }

  @Test
  public void sanitizeHandlesMultiCharNameStartingWithNumber() {
    // Regression test: "1abc" must be sanitized to "_1abc", not left as "1abc".
    // Java String.matches() requires a full-string match, so the pattern must include .*
    // to cover names longer than one character.
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
        List.of("1fieldName"));
    Schema schema = AvroConverter.avro("ns", "Record", dataType);
    assertEquals("_1fieldName", schema.getFields().get(0).name());
  }

  @Test
  public void relUnionNullStringProducesSingleNullableVarcharField() {
    // UNION(NULL, STRING) should collapse to a single nullable VARCHAR, not two fields
    Schema unionSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rel = AvroConverter.rel(unionSchema, typeFactory);

    // Must NOT be a struct (would indicate NULL wasn't filtered out or union wasn't collapsed)
    assertFalse(rel.isStruct(), "UNION(NULL, STRING) should collapse to a scalar, not a struct");
    assertEquals(SqlTypeName.VARCHAR, rel.getSqlTypeName());
    assertTrue(rel.isNullable());
  }

  @Test
  public void relUnionNullIntStringProducesStructWithTwoNonNullFields() {
    // UNION(NULL, INT, STRING) — 3-type union — should produce a struct with INT and STRING fields only
    // The NULL type must be filtered out
    Schema unionSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.STRING));
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rel = AvroConverter.rel(unionSchema, typeFactory);

    assertTrue(rel.isStruct(), "3-type union should become a struct");
    assertEquals(2, rel.getFieldCount(), "NULL type must be filtered out, leaving INT and STRING fields");
    // Neither field should have SQL type NULL
    rel.getFieldList().forEach(f ->
        assertFalse(f.getType().getSqlTypeName() == SqlTypeName.NULL,
            "No field in the result should have SQL type NULL"));
  }

  @Test
  public void relUnionTwoTypesVsThreeTypesProduceDifferentStructures() {
    // 2-type nullable union should produce a scalar; 3-type nullable union should produce a struct
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    Schema twoType = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    RelDataType twoTypeRel = AvroConverter.rel(twoType, typeFactory);
    assertFalse(twoTypeRel.isStruct(), "2-type union (NULL+STRING) should collapse to scalar");

    Schema threeType = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.INT));
    RelDataType threeTypeRel = AvroConverter.rel(threeType, typeFactory);
    assertTrue(threeTypeRel.isStruct(), "3-type union (NULL+STRING+INT) should produce a struct");
    assertEquals(2, threeTypeRel.getFieldCount());
  }

  @Test
  public void avroKeyPayloadSchemaNullKeysReturnsNullKeySchema() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
        List.of("field1"));

    // null keys value in map — keys == null branch
    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema(
        "ns", "key", "payload", dataType, Map.of());

    assertNull(result.getKey(), "null keys should produce null key schema");
    assertNotNull(result.getValue());
  }

  @Test
  public void avroKeyPayloadSchemaEmptyKeysReturnsNullKeySchema() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
        List.of("field1"));

    // empty string keys — keys.isEmpty() branch
    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema(
        "ns", "key", "payload", dataType, Map.of("key.fields", ""));

    assertNull(result.getKey(), "empty keys should produce null key schema");
    assertNotNull(result.getValue());
  }

  @Test
  public void avroKeyPayloadSchemaNonStructTypeReturnsNullKeySchema() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType primitiveType = typeFactory.createSqlType(SqlTypeName.INTEGER);

    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema(
        "ns", "key", "payload", primitiveType, Map.of("key.fields", "someField"));

    assertNull(result.getKey(), "non-struct dataType should produce null key schema");
    assertNotNull(result.getValue());
    assertEquals(Schema.Type.INT, result.getValue().getType());
  }

  @Test
  public void avroKeyPayloadSchemaFieldNotInKeyNamesGoesToPayload() {
    // field "nonKeyField" is NOT in keyNames — must go to payload, not key
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.INTEGER)),
        List.of("keyField", "nonKeyField"));

    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema(
        "ns", "keySchema", "payloadSchema", dataType, Map.of("key.fields", "keyField"));

    assertNotNull(result.getKey());
    assertEquals(1, result.getKey().getFields().size());
    assertEquals("keyField", result.getKey().getFields().get(0).name());

    assertNotNull(result.getValue());
    assertEquals(1, result.getValue().getFields().size());
    assertEquals("nonKeyField", result.getValue().getFields().get(0).name(),
        "Field not in keyNames must appear in payload, not key");
  }

  @Test
  public void avroKeyPayloadSchemaAllFieldsAreKeysReturnsNullPayload() {
    // Every field matches a key name → payloadBuilder stays empty → payloadSchema == null
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.INTEGER)),
        List.of("KEY_field1", "KEY_field2"));

    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema(
        "ns", "keySchema", "payloadSchema", dataType,
        Map.of("key.fields", "KEY_field1;KEY_field2", "key.fields-prefix", "KEY_"));

    assertNotNull(result.getKey());
    assertEquals(2, result.getKey().getFields().size());
    assertNull(result.getValue(), "empty payload should produce null payload schema");
  }

  @Test
  public void avroKeyPayloadSchemaKeyNamesMatchNoFieldsReturnsNullKey() {
    // key.fields names don't match any field in the type → keyBuilder stays empty → keySchema == null
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), typeFactory.createSqlType(SqlTypeName.INTEGER)),
        List.of("field1", "field2"));

    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema(
        "ns", "keySchema", "payloadSchema", dataType,
        Map.of("key.fields", "KEY_nonexistent", "key.fields-prefix", "KEY_"));

    assertNull(result.getKey(), "no matching key fields should produce null key schema");
    assertNotNull(result.getValue());
    assertEquals(2, result.getValue().getFields().size());
  }

  @Test
  public void avroKeyPayloadSchemaPrimitiveKeyWithNoPayloadReturnsNullPayload() {
    // Primitive key takes the only field → payloadBuilder stays empty → payloadSchema == null
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.INTEGER)),
        List.of("KEY"));

    Pair<Schema, Schema> result = AvroConverter.avroKeyPayloadSchema(
        "ns", "keySchema", "payloadSchema", dataType,
        Map.of("key.fields", "KEY"));

    assertNotNull(result.getKey());
    assertEquals(Schema.Type.INT, result.getKey().getType());
    assertNull(result.getValue(), "primitive key with no remaining fields should produce null payload schema");
  }

  @Test
  public void avroFieldDocumentationIsNonEmpty() {
    // describe() returns "fieldName TYPE_STRING" — verify it flows into schema field doc
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR)),
        List.of("myField"));

    Schema avroSchema = AvroConverter.avro("ns", "Record", dataType);

    assertEquals(1, avroSchema.getFields().size());
    String doc = avroSchema.getFields().get(0).doc();
    assertNotNull(doc, "field doc must not be null");
    assertFalse(doc.isEmpty(), "field doc must not be empty");
    assertTrue(doc.contains("myField"), "field doc should contain the field name");
  }

  @Test
  public void avroConvertsUnknownTypedFieldsToNullUnion() {
    // A struct with an UNKNOWN-typed field: avro() converts UNKNOWN → ["null"] union
    // This test validates the if-condition (innerField.isUnion() && innerField.isNullable())
    // which sets the null default value for nullable union fields
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType unknownType = typeFactory.createUnknownType();
    RelDataType dataType = typeFactory.createStructType(
        List.of(typeFactory.createSqlType(SqlTypeName.VARCHAR), unknownType),
        List.of("goodField", "badField"));

    Schema avroSchema = AvroConverter.avro("ns", "Record", dataType);

    // Both fields are present (avro() converts them, not filters them)
    assertEquals(2, avroSchema.getFields().size());
    assertEquals("goodField", avroSchema.getFields().get(0).name());
    assertEquals("badField", avroSchema.getFields().get(1).name());
    // The UNKNOWN field is converted to a nullable union ["null"] and gets NULL_DEFAULT_VALUE
    Schema badFieldSchema = avroSchema.getFields().get(1).schema();
    assertTrue(badFieldSchema.isUnion(), "UNKNOWN field should become a union schema");
    assertTrue(badFieldSchema.isNullable(), "UNKNOWN field union must contain null");
    // The null default value must be set (exercises the innerField.isUnion() && isNullable() branch)
    assertNotNull(avroSchema.getFields().get(1).defaultVal(),
        "Nullable union field must have a non-null default value");
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

    // Verify the schema can be parsed back
    Schema.Parser parser = new Schema.Parser();
    Schema reparsedSchema = parser.parse(schemaJson);
    assertNotNull(reparsedSchema);
  }

  // --- type mapping tests (merged from AvroConverterTypeMappingTest) ---

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  static Stream<Arguments> relToAvroPrimitiveCases() {
    return Stream.of(
        Arguments.of("SMALLINT", SqlTypeName.SMALLINT, Schema.Type.INT),
        Arguments.of("TINYINT", SqlTypeName.TINYINT, Schema.Type.INT),
        Arguments.of("INTEGER", SqlTypeName.INTEGER, Schema.Type.INT),
        Arguments.of("BIGINT", SqlTypeName.BIGINT, Schema.Type.LONG),
        Arguments.of("VARCHAR", SqlTypeName.VARCHAR, Schema.Type.STRING),
        Arguments.of("CHAR", SqlTypeName.CHAR, Schema.Type.STRING),
        Arguments.of("FLOAT", SqlTypeName.FLOAT, Schema.Type.FLOAT),
        Arguments.of("DOUBLE", SqlTypeName.DOUBLE, Schema.Type.DOUBLE),
        Arguments.of("BOOLEAN", SqlTypeName.BOOLEAN, Schema.Type.BOOLEAN)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("relToAvroPrimitiveCases")
  void testAvroFromRelPrimitive(String name, SqlTypeName sqlType, Schema.Type expectedAvroType) {
    RelDataType relType = typeFactory.createSqlType(sqlType);
    Schema avroSchema = AvroConverter.avro("ns", "field", relType);

    assertNotNull(avroSchema);
    assertEquals(expectedAvroType, avroSchema.getType());
    assertFalse(avroSchema.isNullable());
  }

  @ParameterizedTest(name = "{0} nullable")
  @MethodSource("relToAvroPrimitiveCases")
  void testAvroFromRelPrimitiveNullable(String name, SqlTypeName sqlType, Schema.Type expectedAvroType) {
    RelDataType relType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlType), true);
    Schema avroSchema = AvroConverter.avro("ns", "field", relType);

    assertTrue(avroSchema.isUnion());
    assertTrue(avroSchema.isNullable());
    assertTrue(avroSchema.getTypes().stream().anyMatch(t -> t.getType() == expectedAvroType));
    assertTrue(avroSchema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL));
  }

  @Test
  void testAvroFromBinaryWithPrecision() {
    RelDataType relType = typeFactory.createSqlType(SqlTypeName.BINARY, 16);
    Schema avroSchema = AvroConverter.avro("ns", "hash", relType);

    assertNotNull(avroSchema);
    assertEquals(Schema.Type.FIXED, avroSchema.getType());
    assertEquals(16, avroSchema.getFixedSize());
  }

  @Test
  void testAvroFromVarbinaryWithoutPrecision() {
    RelDataType relType = typeFactory.createSqlType(SqlTypeName.VARBINARY);
    Schema avroSchema = AvroConverter.avro("ns", "data", relType);

    assertNotNull(avroSchema);
    assertEquals(Schema.Type.BYTES, avroSchema.getType());
  }

  @Test
  void testAvroFromMap() {
    RelDataType mapType = typeFactory.createMapType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        typeFactory.createSqlType(SqlTypeName.INTEGER));
    Schema avroSchema = AvroConverter.avro("ns", "myMap", mapType);

    assertNotNull(avroSchema);
    assertEquals(Schema.Type.MAP, avroSchema.getType());
    assertEquals(Schema.Type.INT, avroSchema.getValueType().getType());
  }

  @Test
  void testAvroFromNullType() {
    RelDataType nullType = typeFactory.createSqlType(SqlTypeName.NULL);
    Schema avroSchema = AvroConverter.avro("ns", "n", nullType);

    assertNotNull(avroSchema);
    assertTrue(avroSchema.isUnion());
    assertEquals(1, avroSchema.getTypes().size());
    assertEquals(Schema.Type.NULL, avroSchema.getTypes().get(0).getType());
  }

  @Test
  void testAvroFromUnsupportedTypeThrows() {
    RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
    assertThrows(UnsupportedOperationException.class, () -> AvroConverter.avro("ns", "n", anyType));
  }

  static Stream<Arguments> avroToRelPrimitiveCases() {
    return Stream.of(
        Arguments.of("INT", Schema.create(Schema.Type.INT), SqlTypeName.INTEGER),
        Arguments.of("LONG", Schema.create(Schema.Type.LONG), SqlTypeName.BIGINT),
        Arguments.of("STRING", Schema.create(Schema.Type.STRING), SqlTypeName.VARCHAR),
        Arguments.of("FLOAT", Schema.create(Schema.Type.FLOAT), SqlTypeName.FLOAT),
        Arguments.of("DOUBLE", Schema.create(Schema.Type.DOUBLE), SqlTypeName.DOUBLE),
        Arguments.of("BOOLEAN", Schema.create(Schema.Type.BOOLEAN), SqlTypeName.BOOLEAN),
        Arguments.of("BYTES", Schema.create(Schema.Type.BYTES), SqlTypeName.VARBINARY)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("avroToRelPrimitiveCases")
  void testRelFromAvroPrimitive(String name, Schema avroSchema, SqlTypeName expectedSqlType) {
    RelDataType relType = AvroConverter.rel(avroSchema, typeFactory);

    assertNotNull(relType);
    assertEquals(expectedSqlType, relType.getSqlTypeName());
  }

  @Test
  void testRelFromEnum() {
    Schema enumSchema = Schema.createEnum("Color", null, "ns", List.of("RED", "GREEN", "BLUE"));
    RelDataType relType = AvroConverter.rel(enumSchema, typeFactory);

    assertNotNull(relType);
    assertEquals(SqlTypeName.VARCHAR, relType.getSqlTypeName());
  }

  @Test
  void testRelFromFixed() {
    Schema fixedSchema = Schema.createFixed("hash", null, "ns", 16);
    RelDataType relType = AvroConverter.rel(fixedSchema, typeFactory);

    assertNotNull(relType);
    assertEquals(SqlTypeName.VARBINARY, relType.getSqlTypeName());
    assertEquals(16, relType.getPrecision());
  }

  @Test
  void testRelFromMap() {
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
    RelDataType relType = AvroConverter.rel(mapSchema, typeFactory);

    assertNotNull(relType);
    assertNotNull(relType.getValueType());
    assertEquals(SqlTypeName.INTEGER, relType.getValueType().getSqlTypeName());
  }

  @Test
  void testRelFromNullType() {
    Schema nullSchema = Schema.create(Schema.Type.NULL);
    RelDataType relType = AvroConverter.rel(nullSchema, typeFactory);

    assertTrue(relType.isNullable());
    assertEquals(SqlTypeName.NULL, relType.getSqlTypeName());
  }

  @Test
  void testRelFromProtoDataType() {
    Schema avroSchema = Schema.create(Schema.Type.STRING);
    RelDataType relType = AvroConverter.rel(avroSchema);

    assertNotNull(relType);
    assertEquals(SqlTypeName.VARCHAR, relType.getSqlTypeName());
  }

  @Test
  void testProto() {
    Schema avroSchema = Schema.create(Schema.Type.INT);
    RelProtoDataType proto = AvroConverter.proto(avroSchema);
    assertEquals(SqlTypeName.INTEGER, proto.apply(typeFactory).getSqlTypeName());
  }

  @Test
  void testAvroFromRelProtoDataType() {
    Schema avroSchema = Schema.create(Schema.Type.STRING);
    Schema result = AvroConverter.avro("ns", "test", AvroConverter.proto(avroSchema));

    assertEquals(Schema.Type.STRING, result.getType());
  }

  @Test
  void testAvroFromDateUsesLogicalType() {
    RelDataType dateType = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.DATE), true);
    RelDataType rel = typeFactory.createStructType(
        List.of(dateType), List.of("dateField"));

    Schema avroSchema = AvroConverter.avro("NS", "R", rel);
    assertNotNull(avroSchema);
    assertEquals(1, avroSchema.getFields().size());

    Schema fieldSchema = avroSchema.getFields().get(0).schema();
    assertTrue(fieldSchema.isUnion());
    Schema innerSchema = fieldSchema.getTypes().get(1);
    assertEquals(Schema.Type.INT, innerSchema.getType());
    assertNotNull(innerSchema.getLogicalType());
    assertEquals("date", innerSchema.getLogicalType().getName());

    RelDataType relDataTypeAgain = AvroConverter.rel(avroSchema);
    assertEquals(SqlTypeName.DATE,
        Objects.requireNonNull(relDataTypeAgain.getField("dateField", false, false)).getType().getSqlTypeName());
  }

  @Test
  void testAvroFromTimestampUsesLogicalType() {
    RelDataType timestampType = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP), true);
    RelDataType rel = typeFactory.createStructType(
        List.of(timestampType), List.of("timestampField"));

    Schema avroSchema = AvroConverter.avro("NS", "R", rel);
    assertNotNull(avroSchema);
    assertEquals(1, avroSchema.getFields().size());

    Schema fieldSchema = avroSchema.getFields().get(0).schema();
    assertTrue(fieldSchema.isUnion());
    Schema innerSchema = fieldSchema.getTypes().get(1);
    assertEquals(Schema.Type.LONG, innerSchema.getType());
    assertNotNull(innerSchema.getLogicalType());
    assertEquals("timestamp-millis", innerSchema.getLogicalType().getName());

    RelDataType relDataTypeAgain = AvroConverter.rel(avroSchema);
    assertEquals(SqlTypeName.TIMESTAMP,
        Objects.requireNonNull(relDataTypeAgain.getField("timestampField", false, false)).getType().getSqlTypeName());
  }
}
