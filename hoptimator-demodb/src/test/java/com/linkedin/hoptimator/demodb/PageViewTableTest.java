package com.linkedin.hoptimator.demodb;

import java.util.Collection;
import java.util.Iterator;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class PageViewTableTest {

  @Test
  void testConstructorPopulatesRows() {
    PageViewTable table = new PageViewTable();
    Collection<PageViewTable.Row> rows = (Collection<PageViewTable.Row>) table.rows();
    assertEquals(2, rows.size());
  }

  @Test
  void testRowData() {
    PageViewTable table = new PageViewTable();
    Iterator<PageViewTable.Row> iterator =
        ((Collection<PageViewTable.Row>) table.rows()).iterator();

    PageViewTable.Row first = iterator.next();
    assertEquals("urn:li:page:10000", first.PAGE_URN);
    assertEquals("urn:li:member:123", first.MEMBER_URN);

    PageViewTable.Row second = iterator.next();
    assertEquals("urn:li:page:10001", second.PAGE_URN);
    assertEquals("urn:li:member:456", second.MEMBER_URN);
  }

  @Test
  void testRowType() {
    PageViewTable table = new PageViewTable();
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
    assertTrue(rowType.getFieldNames().contains("PAGE_URN"));
    assertTrue(rowType.getFieldNames().contains("MEMBER_URN"));
  }

  @Test
  void testRowConstructor() {
    PageViewTable.Row row = new PageViewTable.Row("urn:p:1", "urn:m:2");
    assertEquals("urn:p:1", row.PAGE_URN);
    assertEquals("urn:m:2", row.MEMBER_URN);
  }
}
