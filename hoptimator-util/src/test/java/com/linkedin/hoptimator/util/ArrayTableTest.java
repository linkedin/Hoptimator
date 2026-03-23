package com.linkedin.hoptimator.util;


import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class ArrayTableTest {

  @Mock
  private DataContext mockDataContext;

  @Test
  void testRowsStartsEmpty() {
    TestArrayTable table = new TestArrayTable();

    assertTrue(table.rows().isEmpty());
  }

  @Test
  void testGetModifiableCollectionReturnsSameAsRows() {
    TestArrayTable table = new TestArrayTable();

    assertNotNull(table.getModifiableCollection());
    assertEquals(table.rows(), table.getModifiableCollection());
  }

  @Test
  void testAddAndRetrieveRows() {
    TestArrayTable table = new TestArrayTable();

    table.rows().add("row1");
    table.rows().add("row2");

    assertEquals(2, table.rows().size());
  }

  @Test
  void testGetElementType() {
    TestArrayTable table = new TestArrayTable();

    assertEquals(String.class, table.getElementType());
  }

  @Test
  void testGetRowTypeReturnsType() {
    TestArrayTable table = new TestArrayTable();

    // getRowType should use copyType
    assertNotNull(table.getRowType(new SqlTypeFactoryImpl(
        RelDataTypeSystem.DEFAULT)));
  }

  @Test
  void testScanReturnsEnumerable() {
    TestArrayTable table = new TestArrayTable();
    table.rows().add("row1");

    Enumerable<Object[]> result = table.scan(mockDataContext);

    assertNotNull(result);
  }

  private static class TestArrayTable extends ArrayTable<String> {
    TestArrayTable() {
      super(String.class);
    }
  }
}
