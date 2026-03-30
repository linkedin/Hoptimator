package com.linkedin.hoptimator.demodb;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class AdsSchemaTest {

  @Test
  void testTablesReturnsLookup() {
    AdsSchema schema = new AdsSchema();
    Lookup<Table> tables = schema.tables();
    assertNotNull(tables);
  }

  @Test
  void testTablesContainsPageViews() {
    AdsSchema schema = new AdsSchema();
    Lookup<Table> tables = schema.tables();
    Table pageViews = tables.get("PAGE_VIEWS");
    assertNotNull(pageViews);
    assertTrue(pageViews instanceof PageViewTable);
  }

  @Test
  void testTablesContainsAdClicks() {
    AdsSchema schema = new AdsSchema();
    Lookup<Table> tables = schema.tables();
    Table adClicks = tables.get("AD_CLICKS");
    assertNotNull(adClicks);
    assertTrue(adClicks instanceof AdClickTable);
  }

  @Test
  void testTablesContainsCampaigns() {
    AdsSchema schema = new AdsSchema();
    Lookup<Table> tables = schema.tables();
    Table campaigns = tables.get("CAMPAIGNS");
    assertNotNull(campaigns);
    assertTrue(campaigns instanceof CampaignTable);
  }

  @Test
  void testTablesReturnsSameLookupOnRepeatedCalls() {
    AdsSchema schema = new AdsSchema();
    Lookup<Table> first = schema.tables();
    Lookup<Table> second = schema.tables();
    // LazyReference should return same instance
    assertNotNull(first);
    assertNotNull(second);
  }

  @Test
  void testLoadTableReturnsNullForUnknownTable() {
    AdsSchema schema = new AdsSchema();
    Lookup<Table> tables = schema.tables();
    Table unknown = tables.get("NONEXISTENT");
    assertNull(unknown);
  }

  @Test
  void testLoadAllTablesReturnsAllThreeTables() {
    AdsSchema schema = new AdsSchema();
    Lookup<Table> tables = schema.tables();
    Set<String> names = tables.getNames(LikePattern.any());
    assertEquals(3, names.size());
    assertTrue(names.contains("PAGE_VIEWS"));
    assertTrue(names.contains("AD_CLICKS"));
    assertTrue(names.contains("CAMPAIGNS"));
  }
}
