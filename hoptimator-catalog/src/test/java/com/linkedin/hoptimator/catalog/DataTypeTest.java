package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


public class DataTypeTest {

  @Test
  public void skipsNestedRows() {
    DataType.Struct struct =
        DataType.struct().with("one", DataType.VARCHAR).with("two", DataType.struct().with("three", DataType.VARCHAR));
    RelDataType row1 = struct.rel();
    assertEquals(row1.toString(), 2, row1.getFieldCount());
    assertNotNull(row1.toString(), row1.getField("one", false, false));
    assertNotNull(row1.toString(), row1.getField("two", false, false));
    RelDataType row2 = struct.dropNestedRows().rel();
    assertEquals(row2.toString(), 1, row2.getFieldCount());
    assertNotNull(row2.toString(), row2.getField("one", false, false));
    assertNull(row2.toString(), row2.getField("two", false, false));
  }
}
