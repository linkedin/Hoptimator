package com.linkedin.hoptimator.mcp.server;

import com.google.gson.Gson;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup do not need resource management")
@ExtendWith(MockitoExtension.class)
class HoptimatorMcpServerTest {

  @Mock
  private Connection mockConnection;

  @Mock
  private DatabaseMetaData mockMetaData;

  @Mock
  private ResultSet mockResultSet;

  @Mock
  private ResultSetMetaData mockResultSetMetaData;

  @Test
  void testConstructorStoresJdbcUrl() {
    HoptimatorMcpServer server = new HoptimatorMcpServer("jdbc:test://localhost");
    assertNotNull(server);
  }

  @Test
  void testConstructorWithToolsAndResources() {
    HoptimatorMcpServer server = new HoptimatorMcpServer(
        "jdbc:test://localhost",
        new ArrayList<>(),
        new ArrayList<>(),
        new ArrayList<>());
    assertNotNull(server);
  }

  // --- isQueryStatement tests ---

  @ParameterizedTest
  @ValueSource(strings = {"SELECT * FROM table", "select count(*) from t", "  SELECT 1"})
  void testIsQueryStatementReturnsTrueForSelectStatements(String sql) {
    assertTrue(HoptimatorMcpServer.isQueryStatement(sql));
  }

  @ParameterizedTest
  @ValueSource(strings = {"CREATE TABLE t (id INT)", "DROP TABLE t", "INSERT INTO t VALUES (1)",
      "UPDATE t SET x=1", "DELETE FROM t"})
  void testIsQueryStatementReturnsFalseForNonSelectStatements(String sql) {
    assertFalse(HoptimatorMcpServer.isQueryStatement(sql));
  }

  // --- isModifyStatement tests ---

  @Test
  void testIsModifyStatementReturnsTrueForCreateMaterializedView() {
    assertTrue(HoptimatorMcpServer.isModifyStatement(
        "CREATE MATERIALIZED VIEW v AS SELECT * FROM t"));
  }

  @Test
  void testIsModifyStatementReturnsTrueForDrop() {
    assertTrue(HoptimatorMcpServer.isModifyStatement("DROP MATERIALIZED VIEW v"));
  }

  @Test
  void testIsModifyStatementReturnsFalseForSelect() {
    assertFalse(HoptimatorMcpServer.isModifyStatement("SELECT * FROM t"));
  }

  @Test
  void testIsModifyStatementReturnsFalseForCreateTable() {
    assertFalse(HoptimatorMcpServer.isModifyStatement("CREATE TABLE t (id INT)"));
  }

  @Test
  void testIsModifyStatementReturnsFalseForCreateViewWithoutMaterialized() {
    assertFalse(HoptimatorMcpServer.isModifyStatement("CREATE VIEW v AS SELECT * FROM t"));
  }

  // --- isQueryableSource tests ---

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT * FROM ADS.PAGE_VIEWS",
      "SELECT * FROM PROFILE.MEMBERS",
      "SELECT * FROM METADATA.TABLES",
      "SELECT * FROM K8S.PIPELINES"
  })
  void testIsQueryableSourceReturnsTrueForKnownSources(String sql) {
    assertTrue(HoptimatorMcpServer.isQueryableSource(sql));
  }

  @Test
  void testIsQueryableSourceReturnsFalseForUnknownSource() {
    assertFalse(HoptimatorMcpServer.isQueryableSource("SELECT * FROM UNKNOWN.TABLE1"));
  }

  // --- collect tests ---

  @Test
  void testCollectEmptyResultSet() throws SQLException {
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
    when(mockResultSet.next()).thenReturn(false);

    List<Map<String, String>> result = HoptimatorMcpServer.collect(mockResultSet);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testCollectWithRows() throws SQLException {
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("NAME");
    when(mockResultSetMetaData.getColumnName(2)).thenReturn("VALUE");
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getObject(1)).thenReturn("row1", "row2");
    when(mockResultSet.getObject(2)).thenReturn("val1", "val2");

    List<Map<String, String>> result = HoptimatorMcpServer.collect(mockResultSet);
    assertEquals(2, result.size());
    assertEquals("row1", result.get(0).get("NAME"));
    assertEquals("val1", result.get(0).get("VALUE"));
    assertEquals("row2", result.get(1).get("NAME"));
    assertEquals("val2", result.get(1).get("VALUE"));
  }

  @Test
  void testCollectWithNullValues() throws SQLException {
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("COL");
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getObject(1)).thenReturn(null);

    List<Map<String, String>> result = HoptimatorMcpServer.collect(mockResultSet);
    assertEquals(1, result.size());
    assertNull(result.get(0).get("COL"));
  }

  // --- getColumns tests ---

  @Test
  void testGetColumnsReturnsColumnInfo() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getColumns(eq("cat"), eq("sch"), eq("tbl"), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString(4)).thenReturn("ID");
    when(mockResultSet.getString(6)).thenReturn("INTEGER");
    when(mockResultSet.getInt(7)).thenReturn(10);
    when(mockResultSet.getInt(10)).thenReturn(10);
    when(mockResultSet.getString(13)).thenReturn(null);

    List<Map<String, Object>> columns = HoptimatorMcpServer.getColumns(mockConnection, "cat", "sch", "tbl");
    assertEquals(1, columns.size());
    assertEquals("ID", columns.get(0).get("COLUMN_NAME"));
    assertEquals("INTEGER", columns.get(0).get("TYPE_NAME"));
    assertEquals(10, columns.get(0).get("COLUMN_SIZE"));
  }

  @Test
  void testGetColumnsEmptyTable() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getColumns(anyString(), anyString(), anyString(), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    List<Map<String, Object>> columns = HoptimatorMcpServer.getColumns(mockConnection, "c", "s", "t");
    assertTrue(columns.isEmpty());
  }

  // --- getPkConstraint tests ---

  @Test
  void testGetPkConstraintReturnsPrimaryKeys() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getPrimaryKeys("cat", "sch", "tbl")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString(4)).thenReturn("ID");
    when(mockResultSet.getString(6)).thenReturn("PK_ID");

    Map<String, Object> constraint = HoptimatorMcpServer.getPkConstraint(mockConnection, "cat", "sch", "tbl");
    assertNotNull(constraint);
    assertEquals("PK_ID", constraint.get("name"));
    @SuppressWarnings("unchecked")
    List<String> columns = (List<String>) constraint.get("constrained_columns");
    assertEquals(1, columns.size());
    assertEquals("ID", columns.get(0));
  }

  @Test
  void testGetPkConstraintReturnsNullWhenNoPrimaryKeys() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getPrimaryKeys("cat", "sch", "tbl")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    Map<String, Object> constraint = HoptimatorMcpServer.getPkConstraint(mockConnection, "cat", "sch", "tbl");
    assertNull(constraint);
  }

  // --- getForeignKeys tests ---

  @Test
  void testGetForeignKeysReturnsForeignKeyInfo() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getImportedKeys("cat", "sch", "tbl")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString(12)).thenReturn("FK_COMPANY");
    when(mockResultSet.getString(1)).thenReturn("ref_cat");
    when(mockResultSet.getString(2)).thenReturn("ref_sch");
    when(mockResultSet.getString(3)).thenReturn("ref_tbl");
    when(mockResultSet.getString(8)).thenReturn("COMPANY_ID");
    when(mockResultSet.getString(4)).thenReturn("ID");

    List<Map<String, Object>> fkeys = HoptimatorMcpServer.getForeignKeys(mockConnection, "cat", "sch", "tbl");
    assertEquals(1, fkeys.size());
    assertEquals("FK_COMPANY", fkeys.get(0).get("name"));
    assertEquals("ref_tbl", fkeys.get(0).get("referred_table"));
  }

  @Test
  void testGetForeignKeysReturnsEmptyListWhenNoForeignKeys() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getImportedKeys("cat", "sch", "tbl")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    List<Map<String, Object>> fkeys = HoptimatorMcpServer.getForeignKeys(mockConnection, "cat", "sch", "tbl");
    assertTrue(fkeys.isEmpty());
  }

  // --- getTableInfo tests ---

  @Test
  void testGetTableInfoCombinesColumnsPksAndFks() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);

    // Columns - need separate ResultSet mocks
    ResultSet columnsRs = mock(ResultSet.class);
    when(mockMetaData.getColumns(eq("cat"), eq("sch"), eq("tbl"), isNull())).thenReturn(columnsRs);
    when(columnsRs.next()).thenReturn(true, false);
    when(columnsRs.getString(4)).thenReturn("ID");
    when(columnsRs.getString(6)).thenReturn("INTEGER");
    when(columnsRs.getInt(7)).thenReturn(10);
    when(columnsRs.getInt(10)).thenReturn(10);
    when(columnsRs.getString(13)).thenReturn(null);

    // Primary keys
    ResultSet pkRs = mock(ResultSet.class);
    when(mockMetaData.getPrimaryKeys("cat", "sch", "tbl")).thenReturn(pkRs);
    when(pkRs.next()).thenReturn(true, false);
    when(pkRs.getString(4)).thenReturn("ID");
    when(pkRs.getString(6)).thenReturn("PK_ID");

    // Foreign keys
    ResultSet fkRs = mock(ResultSet.class);
    when(mockMetaData.getImportedKeys("cat", "sch", "tbl")).thenReturn(fkRs);
    when(fkRs.next()).thenReturn(false);

    Map<String, Object> tableInfo = HoptimatorMcpServer.getTableInfo(mockConnection, "cat", "sch", "tbl", "TABLE");
    assertEquals("cat", tableInfo.get("TABLE_CAT"));
    assertEquals("sch", tableInfo.get("TABLE_SCHEM"));
    assertEquals("tbl", tableInfo.get("TABLE_NAME"));
    assertEquals("TABLE", tableInfo.get("TABLE_TYPE"));
    assertNotNull(tableInfo.get("columns"));
    assertNotNull(tableInfo.get("primary_keys"));
    assertNotNull(tableInfo.get("foreign_keys"));

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> columns = (List<Map<String, Object>>) tableInfo.get("columns");
    assertEquals(true, columns.get(0).get("primary_key"));
  }

  // --- Constants tests ---

  @Test
  void testPipelineStatusQueryIsNotEmpty() {
    assertFalse(HoptimatorMcpServer.PIPELINE_STATUS_QUERY.isEmpty());
  }

  @Test
  void testPipelineElementStatusQueryIsNotEmpty() {
    assertFalse(HoptimatorMcpServer.PIPELINE_ELEMENT_STATUS_QUERY.isEmpty());
  }

  @Test
  void testPipelineDescribeQueryIsNotEmpty() {
    assertFalse(HoptimatorMcpServer.PIPELINE_DESCRIBE_QUERY.isEmpty());
  }

  // --- Additional isModifyStatement tests ---

  @Test
  void testIsModifyStatementReturnsTrueForDropWithoutMaterialized() {
    assertTrue(HoptimatorMcpServer.isModifyStatement("DROP VIEW v"));
  }

  @Test
  void testIsModifyStatementReturnsFalseForInsert() {
    assertFalse(HoptimatorMcpServer.isModifyStatement("INSERT INTO t VALUES (1)"));
  }

  @Test
  void testIsModifyStatementReturnsFalseForUpdate() {
    assertFalse(HoptimatorMcpServer.isModifyStatement("UPDATE t SET x=1"));
  }

  @Test
  void testIsModifyStatementCaseInsensitive() {
    assertTrue(HoptimatorMcpServer.isModifyStatement("create materialized view v AS SELECT * FROM t"));
  }

  @Test
  void testIsModifyStatementWithLeadingWhitespace() {
    assertTrue(HoptimatorMcpServer.isModifyStatement("  DROP MATERIALIZED VIEW v"));
  }

  // --- Additional isQueryStatement tests ---

  @Test
  void testIsQueryStatementReturnsFalseForEmptyishStatements() {
    assertFalse(HoptimatorMcpServer.isQueryStatement("INSERT INTO x SELECT * FROM t"));
  }

  // --- Additional isQueryableSource tests ---

  @Test
  void testIsQueryableSourceCaseInsensitive() {
    assertTrue(HoptimatorMcpServer.isQueryableSource("SELECT * FROM \"K8S\".pipelines"));
  }

  @Test
  void testIsQueryableSourceReturnsFalseForKafka() {
    assertFalse(HoptimatorMcpServer.isQueryableSource("SELECT * FROM KAFKA.TOPIC1"));
  }

  // --- Additional collect tests ---

  @Test
  void testCollectWithMultipleColumnsAndRows() throws SQLException {
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(3);
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("A");
    when(mockResultSetMetaData.getColumnName(2)).thenReturn("B");
    when(mockResultSetMetaData.getColumnName(3)).thenReturn("C");
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getObject(1)).thenReturn("v1");
    when(mockResultSet.getObject(2)).thenReturn(42);
    when(mockResultSet.getObject(3)).thenReturn(null);

    List<Map<String, String>> result = HoptimatorMcpServer.collect(mockResultSet);
    assertEquals(1, result.size());
    assertEquals("v1", result.get(0).get("A"));
    assertEquals("42", result.get(0).get("B"));
    assertNull(result.get(0).get("C"));
  }

  // --- getTableInfo with null PK ---

  @Test
  void testGetTableInfoWithNoPrimaryKeys() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);

    ResultSet columnsRs = mock(ResultSet.class);
    when(mockMetaData.getColumns(eq("cat"), eq("sch"), eq("tbl"), isNull())).thenReturn(columnsRs);
    when(columnsRs.next()).thenReturn(true, false);
    when(columnsRs.getString(4)).thenReturn("NAME");
    when(columnsRs.getString(6)).thenReturn("VARCHAR");
    when(columnsRs.getInt(7)).thenReturn(255);
    when(columnsRs.getInt(10)).thenReturn(0);
    when(columnsRs.getString(13)).thenReturn("default");

    ResultSet pkRs = mock(ResultSet.class);
    when(mockMetaData.getPrimaryKeys("cat", "sch", "tbl")).thenReturn(pkRs);
    when(pkRs.next()).thenReturn(false);

    ResultSet fkRs = mock(ResultSet.class);
    when(mockMetaData.getImportedKeys("cat", "sch", "tbl")).thenReturn(fkRs);
    when(fkRs.next()).thenReturn(false);

    Map<String, Object> tableInfo = HoptimatorMcpServer.getTableInfo(mockConnection, "cat", "sch", "tbl", "TABLE");

    @SuppressWarnings("unchecked")
    List<String> primaryKeys = (List<String>) tableInfo.get("primary_keys");
    assertTrue(primaryKeys.isEmpty());

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> columns = (List<Map<String, Object>>) tableInfo.get("columns");
    assertEquals(false, columns.get(0).get("primary_key"));
    assertEquals("default", columns.get(0).get("COLUMN_DEF"));
  }

  // --- getPkConstraint with multiple columns ---

  @Test
  void testGetPkConstraintWithMultipleColumns() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getPrimaryKeys("cat", "sch", "tbl")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(4)).thenReturn("ID", "NAME");
    when(mockResultSet.getString(6)).thenReturn("PK_COMPOSITE", "PK_COMPOSITE");

    Map<String, Object> constraint = HoptimatorMcpServer.getPkConstraint(mockConnection, "cat", "sch", "tbl");
    assertNotNull(constraint);
    assertEquals("PK_COMPOSITE", constraint.get("name"));
    @SuppressWarnings("unchecked")
    List<String> columns = (List<String>) constraint.get("constrained_columns");
    assertEquals(2, columns.size());
    assertEquals("ID", columns.get(0));
    assertEquals("NAME", columns.get(1));
  }

  // --- getForeignKeys with multiple FKs ---

  @Test
  void testGetForeignKeysWithMultipleForeignKeys() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getImportedKeys("cat", "sch", "tbl")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(12)).thenReturn("FK_A", "FK_B");
    when(mockResultSet.getString(1)).thenReturn("c1", "c2");
    when(mockResultSet.getString(2)).thenReturn("s1", "s2");
    when(mockResultSet.getString(3)).thenReturn("t1", "t2");
    when(mockResultSet.getString(8)).thenReturn("col_a", "col_b");
    when(mockResultSet.getString(4)).thenReturn("ref_a", "ref_b");

    List<Map<String, Object>> fkeys = HoptimatorMcpServer.getForeignKeys(mockConnection, "cat", "sch", "tbl");
    assertEquals(2, fkeys.size());
  }

  // --- getForeignKeys grouping same FK name ---

  @Test
  void testGetForeignKeysGroupsSameFkName() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getImportedKeys("cat", "sch", "tbl")).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(12)).thenReturn("FK_COMPOSITE", "FK_COMPOSITE");
    when(mockResultSet.getString(1)).thenReturn("ref_cat");
    when(mockResultSet.getString(2)).thenReturn("ref_sch");
    when(mockResultSet.getString(3)).thenReturn("ref_tbl");
    when(mockResultSet.getString(8)).thenReturn("COL_A", "COL_B");
    when(mockResultSet.getString(4)).thenReturn("REF_A", "REF_B");

    List<Map<String, Object>> fkeys = HoptimatorMcpServer.getForeignKeys(mockConnection, "cat", "sch", "tbl");
    assertEquals(1, fkeys.size());
    @SuppressWarnings("unchecked")
    List<String> constrainedColumns = (List<String>) fkeys.get(0).get("constrained_columns");
    assertEquals(2, constrainedColumns.size());
    @SuppressWarnings("unchecked")
    List<String> referredColumns = (List<String>) fkeys.get(0).get("referred_columns");
    assertEquals(2, referredColumns.size());
  }

  // --- getColumns with multiple columns ---

  @Test
  void testGetColumnsWithMultipleColumns() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getColumns(eq("cat"), eq("sch"), eq("tbl"), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(4)).thenReturn("ID", "NAME");
    when(mockResultSet.getString(6)).thenReturn("INTEGER", "VARCHAR");
    when(mockResultSet.getInt(7)).thenReturn(10, 255);
    when(mockResultSet.getInt(10)).thenReturn(10, 0);
    when(mockResultSet.getString(13)).thenReturn(null, "N/A");

    List<Map<String, Object>> columns = HoptimatorMcpServer.getColumns(mockConnection, "cat", "sch", "tbl");
    assertEquals(2, columns.size());
    assertEquals("ID", columns.get(0).get("COLUMN_NAME"));
    assertEquals("NAME", columns.get(1).get("COLUMN_NAME"));
    assertEquals("VARCHAR", columns.get(1).get("TYPE_NAME"));
    assertEquals(255, columns.get(1).get("COLUMN_SIZE"));
  }

  // --- Query constants content ---

  @Test
  void testPipelineStatusQueryContainsSelectAndWhere() {
    assertTrue(HoptimatorMcpServer.PIPELINE_STATUS_QUERY.contains("select"));
    assertTrue(HoptimatorMcpServer.PIPELINE_STATUS_QUERY.contains("where"));
  }

  @Test
  void testPipelineDescribeQueryContainsJoin() {
    assertTrue(HoptimatorMcpServer.PIPELINE_DESCRIBE_QUERY.contains("join"));
  }

  // --- Handler tests ---

  @Mock
  private Statement mockStatement;

  @Mock
  private PreparedStatement mockPreparedStatement;

  private final Gson gson = new Gson();

  @Test
  void testHandleFetchSchemasSuccess() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getSchemas(eq("myCat"), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString(1)).thenReturn("mySchema");
    when(mockResultSet.getString(2)).thenReturn("myCat");

    CallToolResult result = HoptimatorMcpServer.handleFetchSchemas(mockConnection, gson, "myCat");
    assertFalse(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("mySchema"));
  }

  @Test
  void testHandleFetchSchemasError() throws SQLException {
    when(mockConnection.getMetaData()).thenThrow(new SQLException("db error"));

    CallToolResult result = HoptimatorMcpServer.handleFetchSchemas(mockConnection, gson, null);
    assertTrue(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("ERROR"));
  }

  @Test
  void testHandleFetchTablesSuccess() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getTables(eq("c"), eq("s"), eq("%"), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString(1)).thenReturn("c");
    when(mockResultSet.getString(2)).thenReturn("s");
    when(mockResultSet.getString(3)).thenReturn("myTable");
    when(mockResultSet.getString(4)).thenReturn("TABLE");

    CallToolResult result = HoptimatorMcpServer.handleFetchTables(mockConnection, gson, "c", "s");
    assertFalse(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("myTable"));
  }

  @Test
  void testHandleFetchTablesError() throws SQLException {
    when(mockConnection.getMetaData()).thenThrow(new SQLException("db error"));

    CallToolResult result = HoptimatorMcpServer.handleFetchTables(mockConnection, gson, null, null);
    assertTrue(result.isError());
  }

  @Test
  void testHandleFetchPipelinesSuccess() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("NAME");
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getObject(1)).thenReturn("pipeline-1");

    CallToolResult result = HoptimatorMcpServer.handleFetchPipelines(mockConnection, gson);
    assertFalse(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("pipeline-1"));
  }

  @Test
  void testHandleFetchPipelinesError() throws SQLException {
    when(mockConnection.createStatement()).thenThrow(new SQLException("db error"));

    CallToolResult result = HoptimatorMcpServer.handleFetchPipelines(mockConnection, gson);
    assertTrue(result.isError());
  }

  @Test
  void testHandleFetchPipelineStatusNotFound() throws SQLException {
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    when(mockResultSet.next()).thenReturn(false);

    CallToolResult result = HoptimatorMcpServer.handleFetchPipelineStatus(mockConnection, gson, "missing");
    assertTrue(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("not found"));
  }

  @Test
  void testHandleFetchPipelineStatusError() throws SQLException {
    when(mockConnection.prepareStatement(anyString())).thenThrow(new SQLException("db error"));

    CallToolResult result = HoptimatorMcpServer.handleFetchPipelineStatus(mockConnection, gson, "pipeline1");
    assertTrue(result.isError());
  }

  @Test
  void testHandleDescribeTableNotFound() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getTables(isNull(), isNull(), eq("missing"), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    CallToolResult result = HoptimatorMcpServer.handleDescribeTable(mockConnection, gson, "missing", null, null);
    assertTrue(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("not found"));
  }

  @Test
  void testHandleDescribeTableError() throws SQLException {
    when(mockConnection.getMetaData()).thenThrow(new SQLException("db error"));

    CallToolResult result = HoptimatorMcpServer.handleDescribeTable(mockConnection, gson, "t", null, null);
    assertTrue(result.isError());
  }

  @Test
  void testHandleDescribePipelineNotFound() throws SQLException {
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    when(mockResultSet.next()).thenReturn(false);

    CallToolResult result = HoptimatorMcpServer.handleDescribePipeline(mockConnection, gson, "missing");
    assertTrue(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("not found"));
  }

  @Test
  void testHandleDescribePipelineError() throws SQLException {
    when(mockConnection.prepareStatement(anyString())).thenThrow(new SQLException("db error"));

    CallToolResult result = HoptimatorMcpServer.handleDescribePipeline(mockConnection, gson, "p1");
    assertTrue(result.isError());
  }

  @Test
  void testHandleDescribePipelineSuccess() throws SQLException {
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(2);
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("name");
    when(mockResultSetMetaData.getColumnName(2)).thenReturn("SQL");
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getObject(1)).thenReturn("my-pipeline");
    when(mockResultSet.getObject(2)).thenReturn("SELECT * FROM t");

    CallToolResult result = HoptimatorMcpServer.handleDescribePipeline(mockConnection, gson, "my-pipeline");
    assertFalse(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("my-pipeline"));
  }

  @Test
  void testHandleQueryInvalidSql() {
    CallToolResult result = HoptimatorMcpServer.handleQuery(mockConnection, gson, "DROP TABLE t");
    assertTrue(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("not a valid query"));
  }

  @Test
  void testHandleQueryNotQueryableSource() {
    CallToolResult result = HoptimatorMcpServer.handleQuery(mockConnection, gson, "SELECT * FROM KAFKA.TOPIC");
    assertTrue(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("not queryable"));
  }

  @Test
  void testHandleQuerySuccess() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("COL");
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getObject(1)).thenReturn("value");

    CallToolResult result = HoptimatorMcpServer.handleQuery(mockConnection, gson, "SELECT * FROM K8S.PIPELINES");
    assertFalse(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("value"));
  }

  @Test
  void testHandleQueryError() throws SQLException {
    when(mockConnection.createStatement()).thenThrow(new SQLException("db error"));

    CallToolResult result = HoptimatorMcpServer.handleQuery(mockConnection, gson, "SELECT * FROM K8S.PIPELINES");
    assertTrue(result.isError());
  }

  @Test
  void testHandleModifyInvalidSql() {
    CallToolResult result = HoptimatorMcpServer.handleModify(mockConnection, gson, "SELECT * FROM t");
    assertTrue(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("not a valid data modification"));
  }

  @Test
  void testHandleModifySuccess() throws SQLException {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeUpdate(anyString())).thenReturn(1);

    CallToolResult result = HoptimatorMcpServer.handleModify(mockConnection, gson, "DROP MATERIALIZED VIEW v");
    assertFalse(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("1 rows modified"));
  }

  @Test
  void testHandleModifyError() throws SQLException {
    when(mockConnection.createStatement()).thenThrow(new SQLException("db error"));

    CallToolResult result = HoptimatorMcpServer.handleModify(mockConnection, gson, "DROP MATERIALIZED VIEW v");
    assertTrue(result.isError());
  }

  @Test
  void testHandleFetchSchemasEmptyResult() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getSchemas(isNull(), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    CallToolResult result = HoptimatorMcpServer.handleFetchSchemas(mockConnection, gson, null);
    assertFalse(result.isError());
    assertEquals("[]", ((TextContent) result.content().get(0)).text());
  }

  @Test
  void testHandleFetchTablesEmpty() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getTables(isNull(), isNull(), eq("%"), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    CallToolResult result = HoptimatorMcpServer.handleFetchTables(mockConnection, gson, null, null);
    assertFalse(result.isError());
    assertEquals("[]", ((TextContent) result.content().get(0)).text());
  }

  @Test
  void testHandleFetchPipelineStatusSuccess() throws SQLException {
    // First prepareStatement call for pipeline status
    PreparedStatement pipelineStmt = mock(PreparedStatement.class);
    ResultSet pipelineRs = mock(ResultSet.class);
    ResultSetMetaData pipelineMeta = mock(ResultSetMetaData.class);
    when(mockConnection.prepareStatement(eq(HoptimatorMcpServer.PIPELINE_STATUS_QUERY))).thenReturn(pipelineStmt);
    when(pipelineStmt.executeQuery()).thenReturn(pipelineRs);
    when(pipelineRs.getMetaData()).thenReturn(pipelineMeta);
    when(pipelineMeta.getColumnCount()).thenReturn(2);
    when(pipelineMeta.getColumnName(1)).thenReturn("name");
    when(pipelineMeta.getColumnName(2)).thenReturn("status");
    when(pipelineRs.next()).thenReturn(true, false);
    when(pipelineRs.getObject(1)).thenReturn("my-pipeline");
    when(pipelineRs.getObject(2)).thenReturn("RUNNING");

    // Second prepareStatement call for element status
    PreparedStatement elementStmt = mock(PreparedStatement.class);
    ResultSet elementRs = mock(ResultSet.class);
    ResultSetMetaData elementMeta = mock(ResultSetMetaData.class);
    when(mockConnection.prepareStatement(eq(HoptimatorMcpServer.PIPELINE_ELEMENT_STATUS_QUERY))).thenReturn(elementStmt);
    when(elementStmt.executeQuery()).thenReturn(elementRs);
    when(elementRs.getMetaData()).thenReturn(elementMeta);
    when(elementMeta.getColumnCount()).thenReturn(4);
    when(elementMeta.getColumnName(1)).thenReturn("name");
    when(elementMeta.getColumnName(2)).thenReturn("ready");
    when(elementMeta.getColumnName(3)).thenReturn("failed");
    when(elementMeta.getColumnName(4)).thenReturn("message");
    when(elementRs.next()).thenReturn(true, false);
    when(elementRs.getObject(1)).thenReturn("element-1");
    when(elementRs.getObject(2)).thenReturn("true");
    when(elementRs.getObject(3)).thenReturn("false");
    when(elementRs.getObject(4)).thenReturn(null);

    CallToolResult result = HoptimatorMcpServer.handleFetchPipelineStatus(mockConnection, gson, "my-pipeline");
    assertFalse(result.isError());
    String text = ((TextContent) result.content().get(0)).text();
    assertTrue(text.contains("my-pipeline"));
    assertTrue(text.contains("elementStatuses"));
    assertTrue(text.contains("element-1"));
  }

  @Test
  void testHandleFetchPipelineStatusElementStatusError() throws SQLException {
    // Pipeline status succeeds
    PreparedStatement pipelineStmt = mock(PreparedStatement.class);
    ResultSet pipelineRs = mock(ResultSet.class);
    ResultSetMetaData pipelineMeta = mock(ResultSetMetaData.class);
    when(mockConnection.prepareStatement(eq(HoptimatorMcpServer.PIPELINE_STATUS_QUERY))).thenReturn(pipelineStmt);
    when(pipelineStmt.executeQuery()).thenReturn(pipelineRs);
    when(pipelineRs.getMetaData()).thenReturn(pipelineMeta);
    when(pipelineMeta.getColumnCount()).thenReturn(1);
    when(pipelineMeta.getColumnName(1)).thenReturn("name");
    when(pipelineRs.next()).thenReturn(true, false);
    when(pipelineRs.getObject(1)).thenReturn("p1");

    // Element status fails
    PreparedStatement elementStmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(eq(HoptimatorMcpServer.PIPELINE_ELEMENT_STATUS_QUERY))).thenReturn(elementStmt);
    when(elementStmt.executeQuery()).thenThrow(new SQLException("element error"));

    CallToolResult result = HoptimatorMcpServer.handleFetchPipelineStatus(mockConnection, gson, "p1");
    assertTrue(result.isError());
    assertTrue(((TextContent) result.content().get(0)).text().contains("element error"));
  }

  @Test
  void testHandleDescribeTableSuccess() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);

    // getTables returns one table
    ResultSet tablesRs = mock(ResultSet.class);
    when(mockMetaData.getTables(eq("cat"), eq("sch"), eq("tbl"), isNull())).thenReturn(tablesRs);
    when(tablesRs.next()).thenReturn(true, false);
    when(tablesRs.getString(1)).thenReturn("cat");
    when(tablesRs.getString(2)).thenReturn("sch");
    when(tablesRs.getString(3)).thenReturn("tbl");
    when(tablesRs.getString(4)).thenReturn("TABLE");

    // getColumns
    ResultSet columnsRs = mock(ResultSet.class);
    when(mockMetaData.getColumns(eq("cat"), eq("sch"), eq("tbl"), isNull())).thenReturn(columnsRs);
    when(columnsRs.next()).thenReturn(true, false);
    when(columnsRs.getString(4)).thenReturn("ID");
    when(columnsRs.getString(6)).thenReturn("INTEGER");
    when(columnsRs.getInt(7)).thenReturn(10);
    when(columnsRs.getInt(10)).thenReturn(10);
    when(columnsRs.getString(13)).thenReturn(null);

    // getPrimaryKeys
    ResultSet pkRs = mock(ResultSet.class);
    when(mockMetaData.getPrimaryKeys("cat", "sch", "tbl")).thenReturn(pkRs);
    when(pkRs.next()).thenReturn(false);

    // getForeignKeys
    ResultSet fkRs = mock(ResultSet.class);
    when(mockMetaData.getImportedKeys("cat", "sch", "tbl")).thenReturn(fkRs);
    when(fkRs.next()).thenReturn(false);

    CallToolResult result = HoptimatorMcpServer.handleDescribeTable(mockConnection, gson, "tbl", "cat", "sch");
    assertFalse(result.isError());
    String text = ((TextContent) result.content().get(0)).text();
    assertTrue(text.contains("tbl"));
    assertTrue(text.contains("INTEGER"));
  }

  @Test
  void testHandleFetchSchemasMultipleSchemas() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getSchemas(isNull(), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(1)).thenReturn("schema1", "schema2");
    when(mockResultSet.getString(2)).thenReturn("cat1", "cat2");

    CallToolResult result = HoptimatorMcpServer.handleFetchSchemas(mockConnection, gson, null);
    assertFalse(result.isError());
    String text = ((TextContent) result.content().get(0)).text();
    assertTrue(text.contains("schema1"));
    assertTrue(text.contains("schema2"));
  }

  @Test
  void testHandleFetchTablesMultipleTables() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getTables(eq("c"), eq("s"), eq("%"), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(1)).thenReturn("c", "c");
    when(mockResultSet.getString(2)).thenReturn("s", "s");
    when(mockResultSet.getString(3)).thenReturn("t1", "t2");
    when(mockResultSet.getString(4)).thenReturn("TABLE", "VIEW");

    CallToolResult result = HoptimatorMcpServer.handleFetchTables(mockConnection, gson, "c", "s");
    assertFalse(result.isError());
    String text = ((TextContent) result.content().get(0)).text();
    assertTrue(text.contains("t1"));
    assertTrue(text.contains("t2"));
  }
}
