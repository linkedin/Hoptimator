package com.linkedin.hoptimator.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.Lookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
@ExtendWith(MockitoExtension.class)
class HoptimatorDatabaseMetaDataTest {

  @Mock
  private CalciteConnection mockCalciteConnection;

  @Mock
  private DatabaseMetaData mockDatabaseMetaData;

  @Mock
  private Statement mockStatement;

  @Mock
  private PreparedStatement mockPreparedStatement;

  private HoptimatorConnection connection;
  private HoptimatorDatabaseMetaData metaData;

  @BeforeEach
  void setUp() throws SQLException {
    Properties props = new Properties();
    connection = new HoptimatorConnection(mockCalciteConnection, props);
    lenient().when(mockCalciteConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
    metaData = new HoptimatorDatabaseMetaData(connection, mockDatabaseMetaData);
  }

  @Test
  void testGetMetaDataReturnsHoptimatorDatabaseMetaData() throws SQLException {
    when(mockCalciteConnection.getMetaData()).thenReturn(mockDatabaseMetaData);

    DatabaseMetaData result = connection.getMetaData();

    assertNotNull(result);
    assertTrue(result instanceof HoptimatorDatabaseMetaData);
  }

  @Test
  void testGetConnectionReturnsHoptimatorConnection() throws SQLException {
    Connection result = metaData.getConnection();

    assertNotNull(result);
    assertSame(connection, result);
  }

  @Test
  void testGetCatalogsExecutesQueryAndReturnsResultSet() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(true, true, false);
    when(mockRs.getString("TABLE_CAT")).thenReturn("catalog1", "catalog2");

    when(mockCalciteConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockRs);

    ResultSet rs = metaData.getCatalogs();

    assertNotNull(rs);
    assertTrue(rs.next());
    assertEquals("catalog1", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("catalog2", rs.getString(1));
    assertFalse(rs.next());
  }

  @Test
  void testGetSchemasReturnsResultSet() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(true, false);
    when(mockRs.getString("TABLE_CATALOG")).thenReturn("cat1");
    when(mockRs.wasNull()).thenReturn(false, false);
    when(mockRs.getString("TABLE_SCHEM")).thenReturn("schema1");

    when(mockCalciteConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockRs);

    ResultSet rs = metaData.getSchemas();

    assertNotNull(rs);
    assertTrue(rs.next());
    assertEquals("schema1", rs.getString("TABLE_SCHEM"));
    assertEquals("cat1", rs.getString("TABLE_CATALOG"));
  }

  @Test
  void testGetSchemasWithCatalogAndPattern() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(true, false);
    when(mockRs.getString("TABLE_CATALOG")).thenReturn("myCatalog");
    when(mockRs.wasNull()).thenReturn(false, false);
    when(mockRs.getString("TABLE_SCHEM")).thenReturn("mySchema");

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockRs);

    ResultSet rs = metaData.getSchemas("myCatalog", null);

    assertNotNull(rs);
    assertTrue(rs.next());
  }

  @Test
  void testGetSchemasWithEmptyCatalog() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(false);

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockRs);

    ResultSet rs = metaData.getSchemas("", null);

    assertNotNull(rs);
  }

  @Test
  void testGetSchemasWithSchemaPattern() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(true, false);
    when(mockRs.getString("TABLE_CATALOG")).thenReturn("cat");
    when(mockRs.wasNull()).thenReturn(false, false);
    when(mockRs.getString("TABLE_SCHEM")).thenReturn("mySchema");

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockRs);

    ResultSet rs = metaData.getSchemas(null, "my%");

    assertNotNull(rs);
    assertTrue(rs.next());
    assertEquals("mySchema", rs.getString("TABLE_SCHEM"));
  }

  @Test
  void testGetSchemasWithSchemaPatternFiltersNonMatching() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(true, false);
    when(mockRs.getString("TABLE_CATALOG")).thenReturn("cat");
    when(mockRs.wasNull()).thenReturn(false, false);
    when(mockRs.getString("TABLE_SCHEM")).thenReturn("other");

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockRs);

    ResultSet rs = metaData.getSchemas(null, "my%");

    assertNotNull(rs);
    assertFalse(rs.next());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetSchemasExpandsCatalogWhenSchemaNull() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(true, false);
    when(mockRs.getString("TABLE_CATALOG")).thenReturn("myCatalog");
    // First wasNull() for catalog = false, second wasNull() for schema = true
    when(mockRs.wasNull()).thenReturn(false, true);
    when(mockRs.getString("TABLE_SCHEM")).thenReturn(null);

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockRs);

    SchemaPlus mockRootSchema = mock(SchemaPlus.class);
    Lookup<SchemaPlus> mockRootSubSchemas = mock(Lookup.class);
    SchemaPlus mockCatalogSchema = mock(SchemaPlus.class);
    Lookup<SchemaPlus> mockCatalogSubSchemas = mock(Lookup.class);

    when(mockCalciteConnection.getRootSchema()).thenReturn(mockRootSchema);
    doReturn(mockRootSubSchemas).when(mockRootSchema).subSchemas();
    when(mockRootSubSchemas.get("myCatalog")).thenReturn(mockCatalogSchema);
    doReturn(mockCatalogSubSchemas).when(mockCatalogSchema).subSchemas();
    doReturn(Set.of("subSchema1")).when(mockCatalogSubSchemas).getNames(any());

    ResultSet rs = metaData.getSchemas(null, null);

    assertNotNull(rs);
    assertTrue(rs.next());
    assertEquals("subSchema1", rs.getString("TABLE_SCHEM"));
    assertEquals("myCatalog", rs.getString("TABLE_CATALOG"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetSchemasExpandsCatalogWithNullSubSchema() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(true, false);
    when(mockRs.getString("TABLE_CATALOG")).thenReturn("missingCatalog");
    when(mockRs.wasNull()).thenReturn(false, true);
    when(mockRs.getString("TABLE_SCHEM")).thenReturn(null);

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockRs);

    SchemaPlus mockRootSchema = mock(SchemaPlus.class);
    Lookup<SchemaPlus> mockRootSubSchemas = mock(Lookup.class);

    when(mockCalciteConnection.getRootSchema()).thenReturn(mockRootSchema);
    doReturn(mockRootSubSchemas).when(mockRootSchema).subSchemas();
    when(mockRootSubSchemas.get("missingCatalog")).thenReturn(null);

    ResultSet rs = metaData.getSchemas(null, null);

    assertNotNull(rs);
    assertFalse(rs.next());
  }

  @Test
  void testGetSchemasNoArgsReturnsResultSet() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(true, false);
    when(mockRs.getString("TABLE_CATALOG")).thenReturn("cat1");
    when(mockRs.wasNull()).thenReturn(false, false);
    when(mockRs.getString("TABLE_SCHEM")).thenReturn("sch1");

    when(mockCalciteConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockRs);

    ResultSet rs = metaData.getSchemas();

    assertNotNull(rs);
    assertTrue(rs.next());
    assertEquals("sch1", rs.getString("TABLE_SCHEM"));
    assertEquals("cat1", rs.getString("TABLE_CATALOG"));
    assertFalse(rs.next());
  }

  @Test
  void testGetSchemasWithNonNullSchemaPatternUsingRegex() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(true, true, false);
    when(mockRs.getString("TABLE_CATALOG")).thenReturn("cat", "cat");
    when(mockRs.wasNull()).thenReturn(false, false, false, false);
    when(mockRs.getString("TABLE_SCHEM")).thenReturn("match_schema", "no_match");

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockRs);

    ResultSet rs = metaData.getSchemas(null, "match%");

    assertNotNull(rs);
    assertTrue(rs.next());
    assertEquals("match_schema", rs.getString("TABLE_SCHEM"));
    assertFalse(rs.next());
  }

  @Test
  void testGetSchemasWithCatalogFiltersSetsParameter() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockRs.next()).thenReturn(false);

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockRs);

    ResultSet rs = metaData.getSchemas("specificCatalog", null);

    assertNotNull(rs);
    verify(mockPreparedStatement).setString(1, "specificCatalog");
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetTablesReturnsResultSet() throws SQLException {
    // Mock getSchemas to return one schema row
    ResultSet mockSchemaRs = mock(ResultSet.class);
    when(mockSchemaRs.next()).thenReturn(true, false);
    when(mockSchemaRs.getString("TABLE_CATALOG")).thenReturn("myCat");
    when(mockSchemaRs.wasNull()).thenReturn(false, false);
    when(mockSchemaRs.getString("TABLE_SCHEM")).thenReturn("mySchema");

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockSchemaRs);

    // Mock schema hierarchy for table lookup
    SchemaPlus mockRootSchema = mock(SchemaPlus.class);
    SchemaPlus mockCatalogSchema = mock(SchemaPlus.class);
    SchemaPlus mockDbSchema = mock(SchemaPlus.class);
    Lookup<SchemaPlus> mockRootSubSchemas = mock(Lookup.class);
    Lookup<SchemaPlus> mockCatSubSchemas = mock(Lookup.class);
    Lookup<Table> mockTablesLookup = mock(Lookup.class);

    when(mockCalciteConnection.getRootSchema()).thenReturn(mockRootSchema);
    doReturn(mockRootSubSchemas).when(mockRootSchema).subSchemas();
    when(mockRootSubSchemas.get("myCat")).thenReturn(mockCatalogSchema);
    doReturn(mockCatSubSchemas).when(mockCatalogSchema).subSchemas();
    when(mockCatSubSchemas.get("mySchema")).thenReturn(mockDbSchema);
    doReturn(mockTablesLookup).when(mockDbSchema).tables();
    doReturn(Set.of("table1")).when(mockTablesLookup).getNames(any());

    Table mockTable = mock(Table.class);
    when(mockTablesLookup.get("table1")).thenReturn(mockTable);
    when(mockTable.getJdbcTableType()).thenReturn(Schema.TableType.TABLE);

    ResultSet rs = metaData.getTables("myCat", "mySchema", null, null);

    assertNotNull(rs);
    assertTrue(rs.next());
    assertEquals("table1", rs.getString("TABLE_NAME"));
    assertEquals("myCat", rs.getString("TABLE_CAT"));
    assertEquals("mySchema", rs.getString("TABLE_SCHEM"));
    assertFalse(rs.next());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetTablesWithTypeFilter() throws SQLException {
    ResultSet mockSchemaRs = mock(ResultSet.class);
    when(mockSchemaRs.next()).thenReturn(true, false);
    when(mockSchemaRs.getString("TABLE_CATALOG")).thenReturn("cat");
    when(mockSchemaRs.wasNull()).thenReturn(false, false);
    when(mockSchemaRs.getString("TABLE_SCHEM")).thenReturn("sch");

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockSchemaRs);

    SchemaPlus mockRootSchema = mock(SchemaPlus.class);
    SchemaPlus mockCatalogSchema = mock(SchemaPlus.class);
    SchemaPlus mockDbSchema = mock(SchemaPlus.class);
    Lookup<SchemaPlus> mockRootSub = mock(Lookup.class);
    Lookup<SchemaPlus> mockCatSub = mock(Lookup.class);
    Lookup<Table> mockTables = mock(Lookup.class);

    when(mockCalciteConnection.getRootSchema()).thenReturn(mockRootSchema);
    doReturn(mockRootSub).when(mockRootSchema).subSchemas();
    when(mockRootSub.get("cat")).thenReturn(mockCatalogSchema);
    doReturn(mockCatSub).when(mockCatalogSchema).subSchemas();
    when(mockCatSub.get("sch")).thenReturn(mockDbSchema);
    doReturn(mockTables).when(mockDbSchema).tables();
    doReturn(Set.of("t1")).when(mockTables).getNames(any());

    Table mockTable = mock(Table.class);
    when(mockTables.get("t1")).thenReturn(mockTable);
    when(mockTable.getJdbcTableType()).thenReturn(Schema.TableType.VIEW);

    // Filter by TABLE type only - should exclude VIEW
    ResultSet rs = metaData.getTables("cat", "sch", null, new String[]{"TABLE"});

    assertNotNull(rs);
    assertFalse(rs.next());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testGetTablesWithMatchingTypeFilter() throws SQLException {
    ResultSet mockSchemaRs = mock(ResultSet.class);
    when(mockSchemaRs.next()).thenReturn(true, false);
    when(mockSchemaRs.getString("TABLE_CATALOG")).thenReturn("cat");
    when(mockSchemaRs.wasNull()).thenReturn(false, false);
    when(mockSchemaRs.getString("TABLE_SCHEM")).thenReturn("sch");

    when(mockCalciteConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockSchemaRs);

    SchemaPlus mockRootSchema = mock(SchemaPlus.class);
    SchemaPlus mockCatalogSchema = mock(SchemaPlus.class);
    SchemaPlus mockDbSchema = mock(SchemaPlus.class);
    Lookup<SchemaPlus> mockRootSub = mock(Lookup.class);
    Lookup<SchemaPlus> mockCatSub = mock(Lookup.class);
    Lookup<Table> mockTables = mock(Lookup.class);

    when(mockCalciteConnection.getRootSchema()).thenReturn(mockRootSchema);
    doReturn(mockRootSub).when(mockRootSchema).subSchemas();
    when(mockRootSub.get("cat")).thenReturn(mockCatalogSchema);
    doReturn(mockCatSub).when(mockCatalogSchema).subSchemas();
    when(mockCatSub.get("sch")).thenReturn(mockDbSchema);
    doReturn(mockTables).when(mockDbSchema).tables();
    doReturn(Set.of("t1")).when(mockTables).getNames(any());

    Table mockTable = mock(Table.class);
    when(mockTables.get("t1")).thenReturn(mockTable);
    when(mockTable.getJdbcTableType()).thenReturn(Schema.TableType.TABLE);

    // Filter by TABLE type - should include
    ResultSet rs = metaData.getTables("cat", "sch", null, new String[]{"TABLE"});

    assertNotNull(rs);
    assertTrue(rs.next());
    assertEquals("t1", rs.getString("TABLE_NAME"));
  }
}
