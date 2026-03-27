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


class CompanyTableTest {

  @Test
  void testConstructorPopulatesRows() {
    CompanyTable table = new CompanyTable();
    Collection<CompanyTable.Row> rows = (Collection<CompanyTable.Row>) table.rows();
    assertEquals(2, rows.size());
  }

  @Test
  void testRowData() {
    CompanyTable table = new CompanyTable();
    Iterator<CompanyTable.Row> iterator =
        ((Collection<CompanyTable.Row>) table.rows()).iterator();

    CompanyTable.Row first = iterator.next();
    assertEquals("LinkedIn", first.COMPANY_NAME);
    assertEquals("urn:li:company:linkedin", first.COMPANY_URN);

    CompanyTable.Row second = iterator.next();
    assertEquals("Microsoft", second.COMPANY_NAME);
    assertEquals("urn:li:company:microsoft", second.COMPANY_URN);
  }

  @Test
  void testRowType() {
    CompanyTable table = new CompanyTable();
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
    assertTrue(rowType.getFieldNames().contains("COMPANY_NAME"));
    assertTrue(rowType.getFieldNames().contains("COMPANY_URN"));
  }

  @Test
  void testRowConstructor() {
    CompanyTable.Row row = new CompanyTable.Row("Acme", "urn:li:company:acme");
    assertEquals("Acme", row.COMPANY_NAME);
    assertEquals("urn:li:company:acme", row.COMPANY_URN);
  }
}
