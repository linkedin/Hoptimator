package com.linkedin.hoptimator.jdbc;

import java.sql.Types;

import org.apache.calcite.avatica.MetaImpl.MetaColumn;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


class HoptimatorMetaColumnFactoryTest {

  private final HoptimatorMetaColumnFactory factory = HoptimatorMetaColumnFactory.INSTANCE;

  private MetaColumn column(int dataType, Integer columnSize, Integer decimalDigits) {
    return factory.createColumn(null, "CAT", "SCH", "TBL", "col", dataType, "TYPE",
        columnSize, decimalDigits, 10, 0, columnSize, 1, "NO");
  }

  @Test
  void testTimestampMirrorsColumnSizeIntoDecimalDigits() {
    MetaColumn c = column(Types.TIMESTAMP, 3, null);
    assertEquals(3, c.decimalDigits);
    assertEquals(3, c.columnSize);
  }

  @Test
  void testTimeMirrorsColumnSizeIntoDecimalDigits() {
    MetaColumn c = column(Types.TIME, 6, null);
    assertEquals(6, c.decimalDigits);
  }

  @Test
  void testTimestampWithTimezoneMirrorsColumnSize() {
    MetaColumn c = column(Types.TIMESTAMP_WITH_TIMEZONE, 9, null);
    assertEquals(9, c.decimalDigits);
  }

  @Test
  void testExistingDecimalDigitsIsPreserved() {
    MetaColumn c = column(Types.TIMESTAMP, 3, 6);
    assertEquals(6, c.decimalDigits);
  }

  @Test
  void testNonTemporalDecimalDigitsUnchanged() {
    MetaColumn varchar = column(Types.VARCHAR, 255, null);
    assertNull(varchar.decimalDigits);

    MetaColumn decimal = column(Types.DECIMAL, 10, 2);
    assertEquals(2, decimal.decimalDigits);
  }

  @Test
  void testTemporalWithNullColumnSizeStaysNull() {
    MetaColumn c = column(Types.TIMESTAMP, null, null);
    assertNull(c.decimalDigits);
  }
}
