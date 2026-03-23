package com.linkedin.hoptimator.util;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class DataTypeUtilsTest {

  private RelDataTypeFactory typeFactory;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  }

  // --- flatten tests ---

  @Test
  void testFlattenNonStructReturnsUnchanged() {
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

    RelDataType result = DataTypeUtils.flatten(intType, typeFactory);

    assertSame(intType, result);
  }

  @Test
  void testFlattenSimpleStructUnchanged() {
    RelDataType struct = typeFactory.builder()
        .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("age", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    RelDataType result = DataTypeUtils.flatten(struct, typeFactory);

    assertEquals(2, result.getFieldCount());
    assertEquals("name", result.getFieldList().get(0).getName());
    assertEquals("age", result.getFieldList().get(1).getName());
  }

  @Test
  void testFlattenNestedStruct() {
    RelDataType inner = typeFactory.builder()
        .add("QUX", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
    RelDataType outer = typeFactory.builder()
        .add("FOO", inner)
        .build();

    RelDataType result = DataTypeUtils.flatten(outer, typeFactory);

    assertEquals(1, result.getFieldCount());
    assertEquals("FOO$QUX", result.getFieldList().get(0).getName());
    assertEquals(SqlTypeName.VARCHAR, result.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testFlattenDeeplyNestedStruct() {
    RelDataType deep = typeFactory.builder()
        .add("C", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();
    RelDataType mid = typeFactory.builder()
        .add("B", deep)
        .build();
    RelDataType outer = typeFactory.builder()
        .add("A", mid)
        .build();

    RelDataType result = DataTypeUtils.flatten(outer, typeFactory);

    assertEquals(1, result.getFieldCount());
    assertEquals("A$B$C", result.getFieldList().get(0).getName());
    assertEquals(SqlTypeName.INTEGER, result.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testFlattenPrimitiveArray() {
    RelDataType arrayType = typeFactory.createArrayType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
    RelDataType struct = typeFactory.builder()
        .add("tags", arrayType)
        .build();

    RelDataType result = DataTypeUtils.flatten(struct, typeFactory);

    assertEquals(1, result.getFieldCount());
    assertEquals("tags", result.getFieldList().get(0).getName());
    assertNotNull(result.getFieldList().get(0).getType().getComponentType());
  }

  @Test
  void testFlattenComplexArrayOfRecords() {
    RelDataType recordType = typeFactory.builder()
        .add("X", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
    RelDataType arrayType = typeFactory.createArrayType(recordType, -1);
    RelDataType struct = typeFactory.builder()
        .add("items", arrayType)
        .build();

    RelDataType result = DataTypeUtils.flatten(struct, typeFactory);

    // Should produce "items ANY ARRAY" and "items$X VARCHAR"
    assertEquals(2, result.getFieldCount());
    assertEquals("items", result.getFieldList().get(0).getName());
    RelDataType componentType = result.getFieldList().get(0).getType().getComponentType();
    assertNotNull(componentType);
    assertEquals(SqlTypeName.ANY, componentType.getSqlTypeName());
    assertEquals("items$X", result.getFieldList().get(1).getName());
    assertEquals(SqlTypeName.VARCHAR, result.getFieldList().get(1).getType().getSqlTypeName());
  }

  @Test
  void testFlattenNestedArray() {
    // Array of arrays: VARCHAR ARRAY ARRAY
    RelDataType innerArray = typeFactory.createArrayType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR), -1);
    RelDataType outerArray = typeFactory.createArrayType(innerArray, -1);
    RelDataType struct = typeFactory.builder()
        .add("matrix", outerArray)
        .build();

    RelDataType result = DataTypeUtils.flatten(struct, typeFactory);

    // Should produce "matrix ANY ARRAY" and "matrix$__ARRTYPE__ VARCHAR ARRAY"
    assertEquals(2, result.getFieldCount());
    assertEquals("matrix", result.getFieldList().get(0).getName());
  }

  @Test
  void testFlattenMapType() {
    RelDataType mapType = typeFactory.createMapType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataType struct = typeFactory.builder()
        .add("props", mapType)
        .build();

    RelDataType result = DataTypeUtils.flatten(struct, typeFactory);

    assertEquals(2, result.getFieldCount());
    assertEquals("props$__MAPKEYTYPE__", result.getFieldList().get(0).getName());
    assertEquals("props$__MAPVALUETYPE__", result.getFieldList().get(1).getName());
  }

  // --- unflatten tests ---

  @Test
  void testUnflattenNonStructThrows() {
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

    assertThrows(IllegalArgumentException.class,
        () -> DataTypeUtils.unflatten(intType, typeFactory));
  }

  @Test
  void testUnflattenSimpleStructUnchanged() {
    RelDataType struct = typeFactory.builder()
        .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("age", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    RelDataType result = DataTypeUtils.unflatten(struct, typeFactory);

    assertEquals(2, result.getFieldCount());
    assertEquals("name", result.getFieldList().get(0).getName());
    assertEquals("age", result.getFieldList().get(1).getName());
  }

  @Test
  void testUnflattenDollarSeparatedFields() {
    RelDataType struct = typeFactory.builder()
        .add("FOO$BAR", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    RelDataType result = DataTypeUtils.unflatten(struct, typeFactory);

    assertEquals(1, result.getFieldCount());
    assertEquals("FOO", result.getFieldList().get(0).getName());
    assertTrue(result.getFieldList().get(0).getType().isStruct());
    assertEquals("BAR",
        result.getFieldList().get(0).getType().getFieldList().get(0).getName());
  }

  @Test
  void testUnflattenDeeplyNested() {
    RelDataType struct = typeFactory.builder()
        .add("A$B$C", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    RelDataType result = DataTypeUtils.unflatten(struct, typeFactory);

    assertEquals(1, result.getFieldCount());
    RelDataType aType = result.getFieldList().get(0).getType();
    assertTrue(aType.isStruct());
    RelDataType bType = aType.getFieldList().get(0).getType();
    assertTrue(bType.isStruct());
    assertEquals("C", bType.getFieldList().get(0).getName());
  }

  @Test
  void testUnflattenComplexArray() {
    // Flatten a complex array, then unflatten it back
    RelDataType recordType = typeFactory.builder()
        .add("X", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
    RelDataType arrayType = typeFactory.createArrayType(recordType, -1);
    RelDataType original = typeFactory.builder()
        .add("items", arrayType)
        .build();

    RelDataType flattened = DataTypeUtils.flatten(original, typeFactory);
    RelDataType unflattened = DataTypeUtils.unflatten(flattened, typeFactory);

    assertEquals(1, unflattened.getFieldCount());
    assertEquals("items", unflattened.getFieldList().get(0).getName());
    assertNotNull(unflattened.getFieldList().get(0).getType().getComponentType());
  }

  @Test
  void testUnflattenMapPlaceholders() {
    RelDataType struct = typeFactory.builder()
        .add("props$__MAPKEYTYPE__", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("props$__MAPVALUETYPE__", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();

    RelDataType result = DataTypeUtils.unflatten(struct, typeFactory);

    assertEquals(1, result.getFieldCount());
    assertEquals("props", result.getFieldList().get(0).getName());
    assertNotNull(result.getFieldList().get(0).getType().getKeyType());
    assertNotNull(result.getFieldList().get(0).getType().getValueType());
  }

  @Test
  void testUnflattenNestedArrayPlaceholder() {
    RelDataType struct = typeFactory.builder()
        .add("matrix$__ARRTYPE__", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();

    RelDataType result = DataTypeUtils.unflatten(struct, typeFactory);

    assertEquals(1, result.getFieldCount());
    assertEquals("matrix", result.getFieldList().get(0).getName());
    assertNotNull(result.getFieldList().get(0).getType().getComponentType());
  }

  @Test
  void testFlattenUnflattenRoundTripNested() {
    RelDataType inner = typeFactory.builder()
        .add("BAR", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("BAZ", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();
    RelDataType original = typeFactory.builder()
        .add("FOO", inner)
        .add("TOP", typeFactory.createSqlType(SqlTypeName.BOOLEAN))
        .build();

    RelDataType flattened = DataTypeUtils.flatten(original, typeFactory);
    RelDataType unflattened = DataTypeUtils.unflatten(flattened, typeFactory);

    assertEquals(2, unflattened.getFieldCount());
    assertEquals("FOO", unflattened.getFieldList().get(0).getName());
    assertTrue(unflattened.getFieldList().get(0).getType().isStruct());
    assertEquals(2, unflattened.getFieldList().get(0).getType().getFieldCount());
    assertEquals("TOP", unflattened.getFieldList().get(1).getName());
  }

  @Test
  void testFlattenMultipleNestedFields() {
    RelDataType inner = typeFactory.builder()
        .add("X", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .add("Y", typeFactory.createSqlType(SqlTypeName.INTEGER))
        .build();
    RelDataType outer = typeFactory.builder()
        .add("A", inner)
        .add("B", typeFactory.createSqlType(SqlTypeName.BOOLEAN))
        .build();

    RelDataType result = DataTypeUtils.flatten(outer, typeFactory);

    assertEquals(3, result.getFieldCount());
    assertEquals("A$X", result.getFieldList().get(0).getName());
    assertEquals("A$Y", result.getFieldList().get(1).getName());
    assertEquals("B", result.getFieldList().get(2).getName());
  }

  @Test
  void testFlattenMapWithNestedValue() {
    RelDataType valueStruct = typeFactory.builder()
        .add("V", typeFactory.createSqlType(SqlTypeName.VARCHAR))
        .build();
    RelDataType mapType = typeFactory.createMapType(
        typeFactory.createSqlType(SqlTypeName.VARCHAR),
        valueStruct);
    RelDataType struct = typeFactory.builder()
        .add("m", mapType)
        .build();

    RelDataType result = DataTypeUtils.flatten(struct, typeFactory);

    assertEquals(2, result.getFieldCount());
    assertEquals("m$__MAPKEYTYPE__", result.getFieldList().get(0).getName());
    assertEquals("m$__MAPVALUETYPE__$V", result.getFieldList().get(1).getName());
  }
}
