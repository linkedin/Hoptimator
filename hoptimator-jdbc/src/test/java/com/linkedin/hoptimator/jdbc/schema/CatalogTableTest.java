package com.linkedin.hoptimator.jdbc.schema;

import com.linkedin.hoptimator.Catalog;
import org.apache.calcite.schema.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class CatalogTableTest {

  @Mock
  private Catalog mockCatalog;

  private CatalogTable catalogTable;

  @BeforeEach
  void setUp() {
    catalogTable = new CatalogTable();
  }

  @Test
  void testGetJdbcTableTypeReturnsSystemTable() {
    assertEquals(Schema.TableType.SYSTEM_TABLE, catalogTable.getJdbcTableType());
  }

  @Test
  void testToRowCreatesCatalogRow() {
    when(mockCatalog.name()).thenReturn("test-catalog");
    when(mockCatalog.description()).thenReturn("A test catalog");

    CatalogTable.Row row = catalogTable.toRow(mockCatalog);

    assertNotNull(row);
    assertEquals("test-catalog", row.NAME);
    assertEquals("A test catalog", row.DESCRIPTION);
  }

  @Test
  void testRowConstructorSetsFields() {
    CatalogTable.Row row = new CatalogTable.Row("myName", "myDescription");

    assertEquals("myName", row.NAME);
    assertEquals("myDescription", row.DESCRIPTION);
  }
}
