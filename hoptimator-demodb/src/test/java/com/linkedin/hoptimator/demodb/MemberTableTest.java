package com.linkedin.hoptimator.demodb;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class MemberTableTest {

  @Test
  void testConstructorPopulatesRows() {
    MemberTable table = new MemberTable();
    Collection<MemberTable.Row> rows = table.rows();
    assertEquals(3, rows.size());
  }

  @Test
  void testRowData() {
    MemberTable table = new MemberTable();
    Iterator<MemberTable.Row> iterator = table.rows().iterator();

    MemberTable.Row alice = iterator.next();
    assertEquals("Alice", alice.FIRST_NAME);
    assertEquals("Addison", alice.LAST_NAME);
    assertEquals("urn:li:member:123", alice.MEMBER_URN);
    assertEquals("urn:li:company:linkedin", alice.COMPANY_URN);

    MemberTable.Row bob = iterator.next();
    assertEquals("Bob", bob.FIRST_NAME);
    assertEquals("Baker", bob.LAST_NAME);

    MemberTable.Row charlie = iterator.next();
    assertEquals("Charlie", charlie.FIRST_NAME);
    assertEquals("Chapman", charlie.LAST_NAME);
    assertEquals("urn:li:company:microsoft", charlie.COMPANY_URN);
  }

  @Test
  void testRowType() {
    MemberTable table = new MemberTable();
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(4, rowType.getFieldCount());
    assertTrue(rowType.getFieldNames().contains("FIRST_NAME"));
    assertTrue(rowType.getFieldNames().contains("LAST_NAME"));
    assertTrue(rowType.getFieldNames().contains("MEMBER_URN"));
    assertTrue(rowType.getFieldNames().contains("COMPANY_URN"));
  }

  @Test
  void testRowConstructor() {
    MemberTable.Row row = new MemberTable.Row("Dave", "Doe", "urn:m:1", "urn:c:1");
    assertEquals("Dave", row.FIRST_NAME);
    assertEquals("Doe", row.LAST_NAME);
    assertEquals("urn:m:1", row.MEMBER_URN);
    assertEquals("urn:c:1", row.COMPANY_URN);
  }
}
