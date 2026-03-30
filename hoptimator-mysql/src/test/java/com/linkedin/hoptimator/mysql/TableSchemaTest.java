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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
}
