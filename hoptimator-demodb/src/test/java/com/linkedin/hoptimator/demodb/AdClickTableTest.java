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


class AdClickTableTest {

  @Test
  void testConstructorPopulatesRows() {
    AdClickTable table = new AdClickTable();
    Collection<AdClickTable.Row> rows = table.rows();
    assertEquals(2, rows.size());
  }

  @Test
  void testRowData() {
    AdClickTable table = new AdClickTable();
    Iterator<AdClickTable.Row> iterator =
        table.rows().iterator();

    AdClickTable.Row first = iterator.next();
    assertEquals("urn:li:campaign:100", first.CAMPAIGN_URN);
    assertEquals("urn:li:member:456", first.MEMBER_URN);

    AdClickTable.Row second = iterator.next();
    assertEquals("urn:li:campaign:101", second.CAMPAIGN_URN);
    assertEquals("urn:li:member:789", second.MEMBER_URN);
  }

  @Test
  void testRowType() {
    AdClickTable table = new AdClickTable();
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
    assertTrue(rowType.getFieldNames().contains("CAMPAIGN_URN"));
    assertTrue(rowType.getFieldNames().contains("MEMBER_URN"));
  }

  @Test
  void testRowConstructor() {
    AdClickTable.Row row = new AdClickTable.Row("urn:c:1", "urn:m:2");
    assertEquals("urn:c:1", row.CAMPAIGN_URN);
    assertEquals("urn:m:2", row.MEMBER_URN);
  }
}
