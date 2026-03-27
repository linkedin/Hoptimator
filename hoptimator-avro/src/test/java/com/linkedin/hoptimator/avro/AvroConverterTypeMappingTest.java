package com.linkedin.hoptimator.avro;

import java.util.List;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class AvroConverterTypeMappingTest {

  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  // --- avro() primitive type mappings ---

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

    assertNotNull(avroSchema);
    assertTrue(avroSchema.isUnion());
    assertTrue(avroSchema.isNullable());
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
    RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
    assertThrows(UnsupportedOperationException.class, () -> AvroConverter.avro("ns", "n", dateType));
  }

  // --- rel() type mappings ---

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

    assertNotNull(relType);
    assertTrue(relType.isNullable());
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
    assertNotNull(AvroConverter.proto(avroSchema));
  }

  @Test
  void testAvroFromRelProtoDataType() {
    Schema avroSchema = Schema.create(Schema.Type.STRING);
    Schema result = AvroConverter.avro("ns", "test", AvroConverter.proto(avroSchema));

    assertNotNull(result);
  }
}
