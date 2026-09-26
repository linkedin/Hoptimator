package com.linkedin.hoptimator.avro;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;


/**
 * Hoptimator's {@link RelDataTypeSystem}. It behaves exactly like Calcite's default system except
 * that it raises the maximum precision of the datetime types from Calcite's hard-coded
 * {@link SqlTypeName#MAX_DATETIME_PRECISION} (3, i.e. milliseconds) up to 9 (nanoseconds), matching
 * what Flink SQL supports. Without this, {@code createSqlType(TIMESTAMP, 6)} — as produced for an
 * Avro {@code timestamp-micros} column — is silently clamped down to {@code TIMESTAMP(3)}, so
 * micro/nanosecond precision could never survive.
 *
 * <p>Exposed via a public {@link #INSTANCE} so it can be named through the {@code typeSystem} Calcite
 * connection property (see {@code HoptimatorDriver}), which routes the whole planner/validator
 * through it. Nothing is forced to a higher precision; the ceiling is merely raised.
 */
public final class HoptimatorTypeSystem extends RelDataTypeSystemImpl {

  public static final RelDataTypeSystem INSTANCE = new HoptimatorTypeSystem();

  // Flink SQL supports TIMESTAMP(0) through TIMESTAMP(9).
  private static final int MAX_DATETIME_PRECISION = 9;

  @Override
  public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return MAX_DATETIME_PRECISION;
      default:
        return super.getMaxPrecision(typeName);
    }
  }
}
