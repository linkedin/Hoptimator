package com.linkedin.hoptimator.avro;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class HoptimatorArraySqlTypeTest {

  @Test
  void testDigestUsesGenerateTypeString() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType elementType = typeFactory.createSqlType(SqlTypeName.INTEGER);

    HoptimatorArraySqlType arrayType = new HoptimatorArraySqlType(elementType, false);

    // The custom computeDigest sets digest to just generateTypeString(true),
    // which should produce a parseable array type string
    String digest = arrayType.toString();
    assertNotNull(digest);
    assertTrue(digest.contains("ARRAY"));
  }

  @Test
  void testNullableArrayDigest() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType elementType = typeFactory.createSqlType(SqlTypeName.FLOAT);

    HoptimatorArraySqlType arrayType = new HoptimatorArraySqlType(elementType, true);

    String digest = arrayType.getFullTypeString();
    assertNotNull(digest);
    assertTrue(digest.contains("FLOAT"));
    assertTrue(digest.contains("ARRAY"));
  }

  @Test
  void testIsNullableReflectsConstructorArg() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType elementType = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    HoptimatorArraySqlType nullableArray = new HoptimatorArraySqlType(elementType, true);
    HoptimatorArraySqlType nonNullableArray = new HoptimatorArraySqlType(elementType, false);

    assertTrue(nullableArray.isNullable());
    assertFalse(nonNullableArray.isNullable());
  }

  @Test
  void testComponentTypePreserved() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType elementType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

    HoptimatorArraySqlType arrayType = new HoptimatorArraySqlType(elementType, false);

    assertNotNull(arrayType.getComponentType());
    assertEquals(SqlTypeName.DOUBLE, arrayType.getComponentType().getSqlTypeName());
  }
}
