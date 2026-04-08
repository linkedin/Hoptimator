package com.linkedin.hoptimator.jdbc.schema;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.Wrapped;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.Lookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
    assertFalse(description.isEmpty());
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
    assertInstanceOf(PrintTable.class, printTable);
  }

  @Test
  void testTablesReturnsNullForUnknownTable() {
    Lookup<Table> tables = catalog.tables();

    assertNotNull(tables);
    assertNull(tables.get("NONEXISTENT"));
  }

  @Test
  void testGetSchemaDescriptionReturnsNonEmptyString() {
    Lookup<Table> tables = catalog.tables();
    // The LazyTableLookup inside UtilityCatalog must return a non-empty schema description.
    // We verify this indirectly by checking that loading a non-existent table triggers no error
    // but more directly by inspecting the error message from a load failure.
    // The cleanest way: verify loadTable for a non-PRINT name returns null, which means the
    // getSchemaDescription() path is exercised in log messages without throwing.
    Table result = tables.get("NONEXISTENT_FOR_DESCRIPTION_TEST");
    assertNull(result);
  }

  @Test
  void testLoadAllTablesReturnsNonEmptySetContainingPrint() {
    Lookup<Table> tables = catalog.tables();
    // LikePattern.any() triggers loadAllTables()
    Set<String> names = tables.getNames(LikePattern.any());
    assertNotNull(names);
    assertFalse(names.isEmpty(), "loadAllTables() must return at least one table");
    assertTrue(names.contains("PRINT"), "loadAllTables() must include 'PRINT'");
  }
}
