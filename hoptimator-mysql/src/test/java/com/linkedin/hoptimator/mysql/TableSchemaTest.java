package com.linkedin.hoptimator.mysql;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.Lookup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.calcite.schema.lookup.LikePattern;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE", "DMI_EMPTY_DB_PASSWORD"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class TableSchemaTest {

  private static final String DATABASE = "test_db";

  @Mock
  private Connection mockConnection;

  @Mock
  private DatabaseMetaData mockMetaData;

  @Mock
  private ResultSet mockResultSet;

  @Mock
  private MockedStatic<DriverManager> driverManagerStatic;

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.setProperty("url", "jdbc:mysql://localhost:3306/test");
    properties.setProperty("user", "testuser");
    properties.setProperty("password", "testpass");
  }

  private void stubConnection() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenReturn(mockConnection);
  }

  @Test
  void tablesGetReturnsTableWhenExists() throws SQLException {
    stubConnection();
    when(mockMetaData.getTables(eq(DATABASE), isNull(), eq("my_table"), any(String[].class)))
        .thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);

    TableSchema schema = new TableSchema(properties, DATABASE);
    Lookup<Table> lookup = schema.tables();
    Table table = lookup.get("my_table");

    assertNotNull(table);
    assertInstanceOf(MySqlTable.class, table);
  }

  @Test
  void tablesGetReturnsNullWhenTableNotFound() throws SQLException {
    stubConnection();
    when(mockMetaData.getTables(eq(DATABASE), isNull(), eq("missing"), any(String[].class)))
        .thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    TableSchema schema = new TableSchema(properties, DATABASE);
    Table table = schema.tables().get("missing");

    assertNull(table);
  }

  @Test
  void tablesGetThrowsOnConnectionError() {
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenThrow(new RuntimeException("Connection refused"));

    TableSchema schema = new TableSchema(properties, DATABASE);
    assertThrows(RuntimeException.class, () -> schema.tables().get("any_table"));
  }

  @Test
  void tablesGetUsesDefaultUserAndPassword() throws SQLException {
    Properties minimal = new Properties();
    minimal.setProperty("url", "jdbc:mysql://localhost:3306/test");

    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    driverManagerStatic.when(() -> DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "", ""))
        .thenReturn(mockConnection);
    when(mockMetaData.getTables(eq(DATABASE), isNull(), eq("t"), any(String[].class)))
        .thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);

    TableSchema schema = new TableSchema(minimal, DATABASE);
    Table table = schema.tables().get("t");
    assertNotNull(table);
  }

  @Test
  void tablesReturnsSameLookupInstance() {
    TableSchema schema = new TableSchema(properties, DATABASE);
    Lookup<Table> first = schema.tables();
    Lookup<Table> second = schema.tables();
    assertEquals(first, second);
  }

  @Test
  void tablesGetNamesReturnsAllTableNames() throws SQLException {
    stubConnection();
    when(mockMetaData.getTables(eq(DATABASE), isNull(), eq("%"), any(String[].class)))
        .thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("TABLE_NAME")).thenReturn("orders", "customers");

    TableSchema schema = new TableSchema(properties, DATABASE);
    Set<String> names = schema.tables().getNames(LikePattern.any());

    assertEquals(Set.of("orders", "customers"), names);
  }

  @Test
  void tablesGetNamesReturnsEmptySetWhenNoTables() throws SQLException {
    stubConnection();
    when(mockMetaData.getTables(eq(DATABASE), isNull(), eq("%"), any(String[].class)))
        .thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    TableSchema schema = new TableSchema(properties, DATABASE);
    Set<String> names = schema.tables().getNames(LikePattern.any());

    assertEquals(Set.of(), names);
  }

  // --- getSchemaDescription() returns non-empty string ---
  // getSchemaDescription() is used inside error messages in LazyTableLookup.
  // When loadTable() throws, the RuntimeException message contains getSchemaDescription().
  // If getSchemaDescription() returns "", the error message would be "Failed to load table 'x' from ".
  // We assert that the error message contains the database name and URL, proving it is non-empty.

  @Test
  void getSchemaDescriptionIsNonEmptyInErrorMessage() {
    // Make the DriverManager throw so that loadTable() throws, triggering the
    // RuntimeException that includes getSchemaDescription() in its message.
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenThrow(new RuntimeException("simulated connection error"));

    TableSchema schema = new TableSchema(properties, DATABASE);
    Lookup<Table> lookup = schema.tables();

    RuntimeException ex = assertThrows(RuntimeException.class, () -> lookup.get("some_table"));

    // The error message should contain the database name and URL from getSchemaDescription()
    // If getSchemaDescription() returned "", the message would NOT contain these strings.
    String message = ex.getMessage();
    assertNotNull(message);
    assertFalse(message.isEmpty());
    // getSchemaDescription() returns "MySQL database 'test_db' at jdbc:mysql://localhost:3306/test"
    assertTrue(message.contains(DATABASE) || message.contains("MySQL database"),
        "Error message should contain database description from getSchemaDescription(), got: " + message);
  }

  @Test
  void getSchemaDescriptionIsNonEmptyInGetNamesErrorMessage() {
    // Make the DriverManager throw during loadAllTables() to trigger the RuntimeException
    // that includes getSchemaDescription() in its error message.
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenThrow(new RuntimeException("simulated connection error"));

    TableSchema schema = new TableSchema(properties, DATABASE);
    Lookup<Table> lookup = schema.tables();

    // getNames(LikePattern.any()) uses "%" which triggers loadAllTables()
    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> lookup.getNames(LikePattern.any()));

    String message = ex.getMessage();
    assertNotNull(message);
    assertFalse(message.isEmpty());
    // If getSchemaDescription() were "", the message would be "Failed to load tables from "
    // With proper implementation it should contain the database/URL info
    assertTrue(message.contains(DATABASE) || message.contains("MySQL database")
            || message.contains("jdbc:mysql"),
        "Error message should reference the schema description, got: " + message);
  }
}
