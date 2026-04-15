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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE", "DMI_EMPTY_DB_PASSWORD"},
    justification = "Mock objects do not hold real resources")
class MySqlCatalogSchemaTest {

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
  void subSchemasGetReturnsTableSchemaWhenDatabaseExists() throws SQLException {
    stubConnection();
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("TABLE_CAT")).thenReturn("mydb");

    MySqlCatalogSchema catalog = new MySqlCatalogSchema(properties);
    Lookup<Schema> subSchemas = catalog.subSchemas();
    Schema schema = subSchemas.get("mydb");

    assertNotNull(schema);
    assertInstanceOf(TableSchema.class, schema);
  }

  @Test
  void subSchemasGetReturnsNullWhenDatabaseNotFound() throws SQLException {
    stubConnection();
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    MySqlCatalogSchema catalog = new MySqlCatalogSchema(properties);
    Schema schema = catalog.subSchemas().get("missing");

    assertNull(schema);
  }

  @Test
  void subSchemasGetIsCaseInsensitive() throws SQLException {
    stubConnection();
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("TABLE_CAT")).thenReturn("MyDatabase");

    MySqlCatalogSchema catalog = new MySqlCatalogSchema(properties);
    Schema schema = catalog.subSchemas().get("mydatabase");

    assertNotNull(schema);
    assertInstanceOf(TableSchema.class, schema);
  }

  @Test
  void subSchemasGetNamesReturnsAllDatabases() throws SQLException {
    stubConnection();
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("TABLE_CAT")).thenReturn("db1", "db2");

    MySqlCatalogSchema catalog = new MySqlCatalogSchema(properties);
    Set<String> names = catalog.subSchemas().getNames(LikePattern.any());

    assertEquals(Set.of("db1", "db2"), names);
  }

  @Test
  void subSchemasGetNamesReturnsEmptySetWhenNoDatabases() throws SQLException {
    stubConnection();
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    MySqlCatalogSchema catalog = new MySqlCatalogSchema(properties);
    Set<String> names = catalog.subSchemas().getNames(LikePattern.any());

    assertTrue(names.isEmpty());
  }

  @Test
  void subSchemasReturnsSameLookupInstance() {
    MySqlCatalogSchema catalog = new MySqlCatalogSchema(properties);
    Lookup<Schema> first = catalog.subSchemas();
    Lookup<Schema> second = catalog.subSchemas();
    assertEquals(first, second);
  }

  @Test
  void subSchemasGetThrowsOnConnectionError() {
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenThrow(new RuntimeException("Connection refused"));

    MySqlCatalogSchema catalog = new MySqlCatalogSchema(properties);
    assertThrows(RuntimeException.class, () -> catalog.subSchemas().get("mydb"));
  }

  @Test
  void subSchemasGetNamesThrowsOnConnectionError() {
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenThrow(new RuntimeException("Connection refused"));

    MySqlCatalogSchema catalog = new MySqlCatalogSchema(properties);
    assertThrows(RuntimeException.class, () -> catalog.subSchemas().getNames(LikePattern.any()));
  }

  @Test
  void subSchemasGetUsesDefaultUserAndPassword() throws SQLException {
    Properties minimal = new Properties();
    minimal.setProperty("url", "jdbc:mysql://localhost:3306");

    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    driverManagerStatic.when(() -> DriverManager.getConnection("jdbc:mysql://localhost:3306", "", ""))
        .thenReturn(mockConnection);
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("TABLE_CAT")).thenReturn("mydb");

    MySqlCatalogSchema catalog = new MySqlCatalogSchema(minimal);
    Schema schema = catalog.subSchemas().get("mydb");

    assertNotNull(schema);
  }

  @Test
  void createTableSchemaReturnsNonNull() {
    MySqlCatalogSchema catalog = new MySqlCatalogSchema(properties);
    TableSchema schema = catalog.createTableSchema("mydb");
    assertNotNull(schema);
  }
}
