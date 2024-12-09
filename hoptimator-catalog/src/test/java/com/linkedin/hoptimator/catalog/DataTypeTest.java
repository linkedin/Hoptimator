package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class DataTypeTest {

  @Test
  public void skipsNestedRows() {
    DataType.Struct struct =
        DataType.struct().with("one", DataType.VARCHAR).with("two", DataType.struct().with("three", DataType.VARCHAR));
    RelDataType row1 = struct.rel();
    assertTrue(row1.toString(), row1.getFieldCount() == 2);
    assertTrue(row1.toString(), row1.getField("one", false, false) != null);
    assertTrue(row1.toString(), row1.getField("two", false, false) != null);
    RelDataType row2 = struct.dropNestedRows().rel();
    assertTrue(row2.toString(), row2.getFieldCount() == 1);
    assertTrue(row2.toString(), row2.getField("one", false, false) != null);
    assertTrue(row2.toString(), row2.getField("two", false, false) == null);
  }
}
