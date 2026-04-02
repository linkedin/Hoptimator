package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class MySqlDeployerTest {

  private static final String DATABASE = "test_db";
  private static final Properties PROPERTIES = new Properties();

  @Mock
  private Connection mockConnection;

  @Mock
  private DatabaseMetaData mockMetaData;

  @Mock
  private Statement mockStatement;

  @Mock
  private HoptimatorConnection mockHoptimatorConnection;

  @Mock
  private MockedStatic<DriverManager> driverManagerStatic;

  @Mock
  private MockedStatic<HoptimatorDriver> hoptimatorDriverStatic;

  static {
    PROPERTIES.setProperty("url", "jdbc:mysql://test-url");
    PROPERTIES.setProperty("user", "testuser");
    PROPERTIES.setProperty("password", "testpass");
  }

  @BeforeEach
  void setUp() throws SQLException {
    // lenient: not all tests open a real connection or use every stub
    lenient().when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    lenient().when(mockConnection.createStatement()).thenReturn(mockStatement);
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenReturn(mockConnection);

    // Mock HoptimatorDriver.rowType to return a simple schema with KEY_id and other fields
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);
  }

  private MySqlDeployer createDeployer(Source source) {
    return new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
  }

  private Validator.Issues collectIssues(MySqlDeployer deployer) {
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);
    return issues;
  }

  // --- create() tests ---

  @Test
  void testCreateNewTable() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "NewTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq(DATABASE), any(), eq("NewTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = createDeployer(source);
    deployer.create();

    // Verify two executeUpdate calls: CREATE DATABASE and CREATE TABLE
    verify(mockStatement, times(2)).executeUpdate(anyString());
    verify(mockConnection).close();
    verify(mockStatement, times(2)).close();
  }

  @Test
  void testCreateExistingTableSkipsCreation() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "ExistingTable"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq(DATABASE), any(), eq("ExistingTable"), any())).thenReturn(existingRs);

    MySqlDeployer deployer = createDeployer(source);
    deployer.create(); // Should not throw, just skip creation

    // CREATE DATABASE is still called even if table exists
    verify(mockStatement, times(1)).executeUpdate(anyString());
    verify(mockConnection).close();
  }

  @Test
  void testCreatePropagatesException() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "ErrorTable"), Collections.emptyMap());

    doThrow(new SQLException("Connection failed"))
        .when(mockMetaData).getTables(eq(DATABASE), any(), eq("ErrorTable"), any());

    MySqlDeployer deployer = createDeployer(source);
    SQLException exception = assertThrows(SQLException.class, deployer::create);

    assertTrue(exception.getMessage().contains("Failed to create table ErrorTable"));

    verify(mockConnection).close();
  }

  // --- delete() tests ---

  @Test
  void testDeleteTable() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "TableToDelete"), Collections.emptyMap());

    // Mock table exists check
    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq(DATABASE), any(), eq("TableToDelete"), any())).thenReturn(existingRs);

    // Mock isDatabaseEmpty check - return false (database has other tables)
    ResultSet emptyCheckRs = mock(ResultSet.class);
    when(emptyCheckRs.next()).thenReturn(true); // Has tables, not empty
    when(mockMetaData.getTables(eq(DATABASE), any(), isNull(), any())).thenReturn(emptyCheckRs);

    MySqlDeployer deployer = createDeployer(source);
    deployer.delete();

    verify(mockStatement).executeUpdate(anyString());
    verify(mockConnection).close();
    verify(mockStatement).close();
  }

  @Test
  void testDeleteTableAndDropEmptyDatabase() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "LastTable"), Collections.emptyMap());

    // Mock table exists check
    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq(DATABASE), any(), eq("LastTable"), any())).thenReturn(existingRs);

    // Mock isDatabaseEmpty check - return true (no more tables)
    ResultSet emptyCheckRs = mock(ResultSet.class);
    when(emptyCheckRs.next()).thenReturn(false); // No tables, database is empty
    when(mockMetaData.getTables(eq(DATABASE), any(), isNull(), any())).thenReturn(emptyCheckRs);

    MySqlDeployer deployer = createDeployer(source);
    deployer.delete();

    // Should execute DROP TABLE and DROP DATABASE (2 calls)
    verify(mockStatement, times(2)).executeUpdate(anyString());
    verify(mockConnection).close();
    verify(mockStatement, times(2)).close();
  }

  @Test
  void testDeleteTableThrowsException() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "ErrorTable"), Collections.emptyMap());

    // Mock table exists check
    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq(DATABASE), any(), eq("ErrorTable"), any())).thenReturn(existingRs);

    doThrow(new SQLException("Delete failed")).when(mockStatement).executeUpdate(anyString());

    MySqlDeployer deployer = createDeployer(source);
    SQLException exception = assertThrows(SQLException.class, deployer::delete);

    assertTrue(exception.getMessage().contains("Failed to delete table ErrorTable"));

    verify(mockConnection).close();
    verify(mockStatement).close();
  }

  // --- validate() tests ---

  @Test
  void testValidatePassesForExistingTable() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "ExistingTable"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq(DATABASE), any(), eq("ExistingTable"), any())).thenReturn(existingRs);

    // Mock primary key check - return "id" to match the KEY_id in the schema
    // Use thenAnswer to create a fresh ResultSet each time getPrimaryKeys is called
    when(mockMetaData.getPrimaryKeys(eq(DATABASE), any(), eq("ExistingTable"))).thenAnswer(invocation -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false); // One PK column: id
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    // Mock existing columns - id is INT (matches the default schema), name is VARCHAR
    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, true, false); // Two columns: id and name
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10, 255);
    when(columnsRs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);
    when(mockMetaData.getColumns(eq(DATABASE), any(), eq("ExistingTable"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertTrue(issues.valid(), "Expected no validation errors for existing table");

    verify(mockConnection).close();
  }

  @Test
  void testValidateReportsConnectionError() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "BrokenTable"), Collections.emptyMap());

    doThrow(new SQLException("Connection failed"))
        .when(mockMetaData).getTables(eq(DATABASE), any(), eq("BrokenTable"), any());

    MySqlDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Failed to validate table"));

    verify(mockConnection).close();
  }

  @Test
  void testValidateFailsWhenKeyFieldTypeChanges() throws Exception {
    // Override the default rowType to have KEY_id as VARCHAR (new type - should fail validation)
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255)); // Changed from INTEGER to VARCHAR
    builder.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255));
    RelDataType newRowType = builder.build();

    // Set up the mock BEFORE creating the deployer
    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(newRowType);

    Source source = new Source("db", List.of("MYSQL", DATABASE, "ExistingTable"), Collections.emptyMap());

    // Mock that table exists
    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq(DATABASE), any(), eq("ExistingTable"), any())).thenReturn(existingRs);

    // Mock primary key check - return "id" to match the KEY_id in the schema
    // Use thenAnswer to create a fresh ResultSet each time getPrimaryKeys is called
    when(mockMetaData.getPrimaryKeys(eq(DATABASE), any(), eq("ExistingTable"))).thenAnswer(invocation -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false); // One PK column: id
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    // Mock existing columns - id is INT (existing type)
    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, true, false); // Two columns: id and name
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10, 255);
    when(columnsRs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);
    when(mockMetaData.getColumns(eq(DATABASE), any(), eq("ExistingTable"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = createDeployer(source);

    Validator.Issues issues = collectIssues(deployer);

    assertFalse(issues.valid(), "Expected validation to fail when KEY field type changes");
    assertTrue(issues.toString().contains("Cannot modify KEY field type"),
        "Expected error message about KEY field type change");
    assertTrue(issues.toString().contains("id"), "Expected error to mention the 'id' field");
    assertTrue(issues.toString().contains("INT"), "Expected error to mention existing INT type");
    assertTrue(issues.toString().contains("VARCHAR"), "Expected error to mention new VARCHAR type");

    verify(mockConnection).close();
  }

  // --- update() tests ---

  @Test
  void testUpdateCallsCreate() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "NewTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq(DATABASE), any(), eq("NewTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = createDeployer(source);
    deployer.update();

    // Verify two executeUpdate calls: CREATE DATABASE and CREATE TABLE
    verify(mockStatement, times(2)).executeUpdate(anyString());
    verify(mockConnection).close();
    verify(mockStatement, times(2)).close();
  }

  // --- restore() tests ---

  @Test
  void testRestoreNoOpWhenNotCreated() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "TestTable"), Collections.emptyMap());
    MySqlDeployer deployer = createDeployer(source);

    deployer.restore();
  }

  @Test
  void testRestoreLogsWarningAfterCreate() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "TestTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq(DATABASE), any(), eq("TestTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = createDeployer(source);
    deployer.create();
    deployer.restore();

    verify(mockConnection).close();
    // Two close calls from create (CREATE DATABASE and CREATE TABLE statements)
    verify(mockStatement, times(2)).close();
  }

  // --- specify() tests ---

  @Test
  void testSpecifyReturnsEmptyList() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "TestTable"), Collections.emptyMap());
    MySqlDeployer deployer = createDeployer(source);

    assertTrue(deployer.specify().isEmpty());
  }

  // --- getConnection() error handling ---

  @Test
  void testCreateFailsWithMissingUrl() throws Exception {
    Source source = new Source("db", List.of("MYSQL", DATABASE, "TestTable"), Collections.emptyMap());
    Properties emptyProps = new Properties();
    MySqlDeployer deployer = new MySqlDeployer(source, emptyProps, mockHoptimatorConnection);

    SQLException exception = assertThrows(SQLException.class, deployer::create);
    assertTrue(exception.getMessage().contains("Failed to create table TestTable")
        || exception.getCause() != null && exception.getCause().getMessage().contains("Missing required property 'url'"));
  }
}
