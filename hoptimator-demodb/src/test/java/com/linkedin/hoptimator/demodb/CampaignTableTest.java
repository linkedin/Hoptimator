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


class CampaignTableTest {

  @Test
  void testConstructorPopulatesRows() {
    CampaignTable table = new CampaignTable();
    Collection<CampaignTable.Row> rows = (Collection<CampaignTable.Row>) table.rows();
    assertEquals(2, rows.size());
  }

  @Test
  void testRowData() {
    CampaignTable table = new CampaignTable();
    Iterator<CampaignTable.Row> iterator =
        ((Collection<CampaignTable.Row>) table.rows()).iterator();

    CampaignTable.Row first = iterator.next();
    assertEquals("urn:li:campaign:100", first.CAMPAIGN_URN);
    assertEquals("urn:li:company:microsoft", first.COMPANY_URN);

    CampaignTable.Row second = iterator.next();
    assertEquals("urn:li:campaign:101", second.CAMPAIGN_URN);
    assertEquals("urn:li:company:microsoft", second.COMPANY_URN);
  }

  @Test
  void testRowType() {
    CampaignTable table = new CampaignTable();
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
    assertTrue(rowType.getFieldNames().contains("CAMPAIGN_URN"));
    assertTrue(rowType.getFieldNames().contains("COMPANY_URN"));
  }

  @Test
  void testRowConstructor() {
    CampaignTable.Row row = new CampaignTable.Row("urn:c:1", "urn:co:2");
    assertEquals("urn:c:1", row.CAMPAIGN_URN);
    assertEquals("urn:co:2", row.COMPANY_URN);
  }
}
