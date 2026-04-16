package com.linkedin.hoptimator.mysql;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.lookup.LikePattern;
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
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE", "DMI_EMPTY_DB_PASSWORD"},
    justification = "Mock objects do not hold real resources")
class MySqlRootSchemaTest {

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
    properties.setProperty("url", "jdbc:mysql://localhost:3306");
    properties.setProperty("user", "testuser");
    properties.setProperty("password", "testpass");
  }

  private void stubConnection() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenReturn(mockConnection);
  }

  @Test
  void noConnectionOpenedOnConstruction() {
    // Construction must not open any MySQL connection.
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenThrow(new RuntimeException("Should not connect at construction time"));
    new MySqlRootSchema(properties); // must not throw
  }

  @Test
  void subSchemasGetReturnsTableSchemaWhenDatabaseExists() throws SQLException {
    stubConnection();
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("TABLE_CAT")).thenReturn("mydb");

    MySqlRootSchema schema = new MySqlRootSchema(properties);
    Schema result = schema.subSchemas().get("mydb");

    assertNotNull(result);
    assertInstanceOf(TableSchema.class, result);
  }

  @Test
  void subSchemasGetReturnsNullWhenDatabaseNotFound() throws SQLException {
    stubConnection();
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    MySqlRootSchema schema = new MySqlRootSchema(properties);
    assertNull(schema.subSchemas().get("missing"));
  }

  @Test
  void subSchemasGetIsCaseInsensitive() throws SQLException {
    stubConnection();
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("TABLE_CAT")).thenReturn("MyDatabase");

    MySqlRootSchema schema = new MySqlRootSchema(properties);
    assertNotNull(schema.subSchemas().get("mydatabase"));
  }

  @Test
  void subSchemasGetNamesReturnsAllDatabases() throws SQLException {
    stubConnection();
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("TABLE_CAT")).thenReturn("db1", "db2");

    MySqlRootSchema schema = new MySqlRootSchema(properties);
    Set<String> names = schema.subSchemas().getNames(LikePattern.any());

    assertEquals(Set.of("db1", "db2"), names);
  }

  @Test
  void subSchemasGetNamesReturnsEmptySetWhenNoDatabases() throws SQLException {
    stubConnection();
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    MySqlRootSchema schema = new MySqlRootSchema(properties);
    assertTrue(schema.subSchemas().getNames(LikePattern.any()).isEmpty());
  }

  @Test
  void subSchemasReturnsSameLookupInstance() {
    MySqlRootSchema schema = new MySqlRootSchema(properties);
    Lookup<Schema> first = schema.subSchemas();
    Lookup<Schema> second = schema.subSchemas();
    assertEquals(first, second);
  }

  @Test
  void subSchemasGetThrowsOnConnectionError() {
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenThrow(new RuntimeException("Connection refused"));

    MySqlRootSchema schema = new MySqlRootSchema(properties);
    assertThrows(RuntimeException.class, () -> schema.subSchemas().get("mydb"));
  }

  @Test
  void subSchemasGetNamesThrowsOnConnectionError() {
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenThrow(new RuntimeException("Connection refused"));

    MySqlRootSchema schema = new MySqlRootSchema(properties);
    assertThrows(RuntimeException.class, () -> schema.subSchemas().getNames(LikePattern.any()));
  }

  @Test
  void subSchemasUsesDefaultUserAndPassword() throws SQLException {
    Properties minimal = new Properties();
    minimal.setProperty("url", "jdbc:mysql://localhost:3306");

    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    driverManagerStatic.when(() -> DriverManager.getConnection("jdbc:mysql://localhost:3306", "", ""))
        .thenReturn(mockConnection);
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("TABLE_CAT")).thenReturn("mydb");

    MySqlRootSchema schema = new MySqlRootSchema(minimal);
    assertNotNull(schema.subSchemas().get("mydb"));
  }

  @Test
  void createTableSchemaReturnsNonNull() {
    MySqlRootSchema schema = new MySqlRootSchema(properties);
    assertNotNull(schema.createTableSchema("mydb"));
  }
}
