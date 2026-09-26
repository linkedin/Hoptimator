package com.linkedin.hoptimator.jdbc;

import java.sql.Types;
import java.util.List;

import org.apache.calcite.avatica.MetaImpl.MetaColumn;
import org.apache.calcite.jdbc.CalciteMetaColumnFactory;
import org.apache.calcite.jdbc.CalciteMetaColumnFactoryImpl;
import org.apache.calcite.schema.Table;

import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * A {@link CalciteMetaColumnFactory} that reports a temporal column's fractional-seconds precision in
 * {@code DECIMAL_DIGITS}.
 *
 * <p>Calcite's default metadata producer ({@code CalciteMetaImpl.columns}) puts a {@code TIMESTAMP}/
 * {@code TIME} column's precision in {@code COLUMN_SIZE} and leaves {@code DECIMAL_DIGITS} null (a
 * temporal type reports {@code allowsScale() == false}). But Calcite's JDBC federation reader
 * ({@code JdbcSchema.getRelDataType}) reads a temporal column's precision from {@code DECIMAL_DIGITS}.
 * So across a Calcite-over-Calcite JDBC federation the two halves disagree and temporal precision is
 * lost — {@code TIMESTAMP(3)} surfaces as {@code TIMESTAMP(0)}. (External JDBC drivers avoid this by
 * populating {@code DECIMAL_DIGITS}, which is where the reader looks.)
 *
 * <p>This factory mirrors {@code COLUMN_SIZE} into {@code DECIMAL_DIGITS} for temporal types (when the
 * producer left it null), so Calcite's own reader picks the precision up over the normal federation
 * path — no bypass required. It is wired in via the {@code metaColumnFactory} connection property (see
 * {@code CalciteDriver}).
 *
 * <p>TODO: {@code JdbcSchema.getRelDataType} builds its proto type with {@code RelDataTypeSystem.DEFAULT}
 * (max datetime precision 3), so precision beyond milliseconds is still clamped there; this factory
 * restores millisecond precision across the federation, which is as far as that Calcite path allows.
 */
public final class HoptimatorMetaColumnFactory implements CalciteMetaColumnFactory {

  public static final HoptimatorMetaColumnFactory INSTANCE = new HoptimatorMetaColumnFactory();

  private final CalciteMetaColumnFactory delegate = CalciteMetaColumnFactoryImpl.INSTANCE;

  public HoptimatorMetaColumnFactory() {
  }

  @Override
  public MetaColumn createColumn(Table table, String tableCat, String tableSchem, String tableName,
      String columnName, int dataType, String typeName, Integer columnSize,
      @Nullable Integer decimalDigits, int numPrecRadix, int nullable, Integer charOctetLength,
      int ordinalPosition, String isNullable) {
    Integer effectiveDecimalDigits = decimalDigits;
    if (decimalDigits == null && columnSize != null && hasFractionalSeconds(dataType)) {
      effectiveDecimalDigits = columnSize;
    }
    return delegate.createColumn(table, tableCat, tableSchem, tableName, columnName, dataType, typeName,
        columnSize, effectiveDecimalDigits, numPrecRadix, nullable, charOctetLength, ordinalPosition,
        isNullable);
  }

  @Override
  public List<String> getColumnNames() {
    return delegate.getColumnNames();
  }

  @Override
  public Class<? extends MetaColumn> getMetaColumnClass() {
    return delegate.getMetaColumnClass();
  }

  private static boolean hasFractionalSeconds(int dataType) {
    switch (dataType) {
      case Types.TIME:
      case Types.TIME_WITH_TIMEZONE:
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return true;
      default:
        return false;
    }
  }
}
