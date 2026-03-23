package com.linkedin.hoptimator.jdbc.schema;

import java.sql.SQLException;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.Lookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.Wrapped;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class UtilityCatalogTest {

  @Mock
  private HoptimatorConnection mockConnection;

  @Mock
  private SchemaPlus mockSchemaPlus;

  private UtilityCatalog catalog;

  @BeforeEach
  void setUp() {
    catalog = new UtilityCatalog();
  }

  @Test
  void testNameReturnsUtil() {
    assertEquals("util", catalog.name());
  }

  @Test
  void testDescriptionIsNotEmpty() {
    String description = catalog.description();

    assertNotNull(description);
    assertTrue(description.length() > 0);
  }

  @Test
  void testRegisterAddsToSchema() throws SQLException {
    Wrapped wrapped = new Wrapped(mockConnection, mockSchemaPlus);
    when(mockSchemaPlus.add("UTIL", catalog)).thenReturn(mockSchemaPlus);

    catalog.register(wrapped);

    verify(mockSchemaPlus).add("UTIL", catalog);
  }

  @Test
  void testTablesContainsPrint() {
    Lookup<Table> tables = catalog.tables();

    assertNotNull(tables);
    Table printTable = tables.get("PRINT");
    assertNotNull(printTable);
    assertTrue(printTable instanceof PrintTable);
  }

  @Test
  void testTablesReturnsNullForUnknownTable() {
    Lookup<Table> tables = catalog.tables();

    assertNotNull(tables);
    assertNull(tables.get("NONEXISTENT"));
  }
}
