package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


public class DataTypeTest {

  @Test
  public void skipsNestedRows() {
    DataType.Struct struct =
        DataType.struct().with("one", DataType.VARCHAR).with("two", DataType.struct().with("three", DataType.VARCHAR));
    RelDataType row1 = struct.rel();
    assertEquals(2, row1.getFieldCount());
    assertNotNull(row1.getField("one", false, false));
    assertNotNull(row1.getField("two", false, false));
    RelDataType row2 = struct.dropNestedRows().rel();
    assertEquals(1, row2.getFieldCount());
    assertNotNull(row2.getField("one", false, false));
    assertNull(row2.getField("two", false, false));
  }
}
