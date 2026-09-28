package com.linkedin.hoptimator.avro;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class HoptimatorTypeSystemTest {

  private final RelDataTypeSystem typeSystem = HoptimatorTypeSystem.INSTANCE;
  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(typeSystem);

  @Test
  void testMaxTimestampPrecisionIsNine() {
    assertEquals(9, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP));
  }

  @Test
  void testMaxTimePrecisionIsNine() {
    assertEquals(9, typeSystem.getMaxPrecision(SqlTypeName.TIME));
  }

  @Test
  void testMaxTimestampWithLocalTimeZonePrecisionIsNine() {
    assertEquals(9, typeSystem.getMaxPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
  }

  @Test
  void testNonDatetimePrecisionDelegatesToDefault() {
    assertEquals(RelDataTypeSystem.DEFAULT.getMaxPrecision(SqlTypeName.VARCHAR),
        typeSystem.getMaxPrecision(SqlTypeName.VARCHAR));
    assertEquals(RelDataTypeSystem.DEFAULT.getMaxPrecision(SqlTypeName.DECIMAL),
        typeSystem.getMaxPrecision(SqlTypeName.DECIMAL));
  }

  @Test
  void testFactoryPreservesMicrosPrecision() {
    RelDataType type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6);
    assertEquals(6, type.getPrecision());
  }

  @Test
  void testFactoryPreservesNanosPrecision() {
    RelDataType type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 9);
    assertEquals(9, type.getPrecision());
  }

  @Test
  void testFactoryClampsBeyondNanosToNine() {
    RelDataType type = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 12);
    assertEquals(9, type.getPrecision());
  }

  @Test
  void testDefaultTypeSystemClampsMicrosToMillis() {
    // Guards the premise for the custom type system: the default clamps micros down to millis.
    RelDataTypeFactory defaultFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    assertEquals(3, defaultFactory.createSqlType(SqlTypeName.TIMESTAMP, 6).getPrecision());
  }
}
