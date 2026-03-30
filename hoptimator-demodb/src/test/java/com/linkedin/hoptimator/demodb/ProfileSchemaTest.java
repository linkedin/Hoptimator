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


class ProfileSchemaTest {

  @Test
  void testTablesReturnsLookup() {
    ProfileSchema schema = new ProfileSchema();
    Lookup<Table> tables = schema.tables();
    assertNotNull(tables);
  }

  @Test
  void testTablesContainsMembers() {
    ProfileSchema schema = new ProfileSchema();
    Lookup<Table> tables = schema.tables();
    Table members = tables.get("MEMBERS");
    assertNotNull(members);
    assertTrue(members instanceof MemberTable);
  }

  @Test
  void testTablesContainsCompanies() {
    ProfileSchema schema = new ProfileSchema();
    Lookup<Table> tables = schema.tables();
    Table companies = tables.get("COMPANIES");
    assertNotNull(companies);
    assertTrue(companies instanceof CompanyTable);
  }

  @Test
  void testTablesReturnsSameLookupOnRepeatedCalls() {
    ProfileSchema schema = new ProfileSchema();
    Lookup<Table> first = schema.tables();
    Lookup<Table> second = schema.tables();
    assertNotNull(first);
    assertNotNull(second);
  }

  @Test
  void testLoadTableReturnsNullForUnknownTable() {
    ProfileSchema schema = new ProfileSchema();
    Lookup<Table> tables = schema.tables();
    Table unknown = tables.get("NONEXISTENT");
    assertNull(unknown);
  }

  @Test
  void testLoadAllTablesReturnsAllTwoTables() {
    ProfileSchema schema = new ProfileSchema();
    Lookup<Table> tables = schema.tables();
    Set<String> names = tables.getNames(LikePattern.any());
    assertEquals(2, names.size());
    assertTrue(names.contains("MEMBERS"));
    assertTrue(names.contains("COMPANIES"));
  }
}
