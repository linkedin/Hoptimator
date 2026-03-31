package com.linkedin.hoptimator.mysql;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE", "DMI_EMPTY_DB_PASSWORD"},
    justification = "Mock objects do not hold real resources")
class MySqlDriverTest {

  @Mock
  private Connection mockMySqlConnection;

  @Mock
  private DatabaseMetaData mockMetaData;

  @Mock
  private ResultSet mockResultSet;

  @Mock
  private TableSchema mockTableSchema;

  @Test
  void testGetConnectStringPrefix() {
    MySqlDriver driver = new MySqlDriver();
    assertEquals("jdbc:mysql-hoptimator://", driver.getConnectStringPrefix());
  }

  @Test
  void testCreateDriverVersion() {
    MySqlDriver driver = new MySqlDriver();
    assertNotNull(driver.createDriverVersion());
  }

  @Test
  void testConnectWithWrongPrefixReturnsNull() throws SQLException {
    MySqlDriver driver = new MySqlDriver();
    Properties props = new Properties();
    assertNull(driver.connect("jdbc:other://localhost", props));
  }

  @Test
  void connectWithCorrectPrefixReturnsConnection() throws SQLException {
    when(mockMySqlConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("TABLE_CAT")).thenReturn("testdb");

    MySqlDriver driver = new MySqlDriver() {
      @Override
      protected Connection createMySqlConnection(String url, String user, String password) {
        return mockMySqlConnection;
      }

      @Override
      protected TableSchema createTableSchema(Properties properties, String schemaName) {
        return mockTableSchema;
      }
    };

    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");
    Connection connection = driver.connect("jdbc:mysql-hoptimator://", props);
    assertNotNull(connection);
    assertInstanceOf(CalciteConnection.class, connection);
    assertTrue(connection.getAutoCommit());
    assertEquals("MYSQL", connection.getCatalog());
    connection.close();
  }

  @Test
  void connectRegistersSchemaForEachCatalog() throws SQLException {
    when(mockMySqlConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("TABLE_CAT")).thenReturn("db1", "db2");

    MySqlDriver driver = new MySqlDriver() {
      @Override
      protected Connection createMySqlConnection(String url, String user, String password) {
        return mockMySqlConnection;
      }

      @Override
      protected TableSchema createTableSchema(Properties properties, String schemaName) {
        return mockTableSchema;
      }
    };

    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");
    Connection connection = driver.connect("jdbc:mysql-hoptimator://", props);
    CalciteConnection calciteConnection = (CalciteConnection) connection;
    assertNotNull(calciteConnection.getRootSchema().subSchemas().get("db1"));
    assertNotNull(calciteConnection.getRootSchema().subSchemas().get("db2"));
    connection.close();
  }

  @Test
  void connectWithMissingUrlThrowsSQLException() {
    MySqlDriver driver = new MySqlDriver();
    Properties props = new Properties();
    SQLException exception = assertThrows(SQLException.class,
        () -> driver.connect("jdbc:mysql-hoptimator://", props));
    assertTrue(exception.getMessage().contains("Missing required parameter 'url'"));
  }

  @Test
  void connectWithMySqlConnectionErrorThrowsSQLException() {
    MySqlDriver driver = new MySqlDriver() {
      @Override
      protected Connection createMySqlConnection(String url, String user, String password)
          throws SQLException {
        throw new SQLException("Connection refused");
      }
    };

    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");
    assertThrows(SQLException.class,
        () -> driver.connect("jdbc:mysql-hoptimator://", props));
  }

  @Test
  void createMySqlConnectionWithBadUrlThrows() {
    MySqlDriver driver = new MySqlDriver();
    assertThrows(SQLException.class,
        () -> driver.createMySqlConnection("jdbc:invalid://nowhere", "", ""));
  }

  @Test
  void createTableSchemaReturnsNonNull() {
    MySqlDriver driver = new MySqlDriver();
    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");
    TableSchema schema = driver.createTableSchema(props, "testdb");
    assertNotNull(schema);
  }

  // --- connect() setAutoCommit: verify autocommit is true ---

  @Test
  void connectSetsAutoCommitToTrue() throws SQLException {
    when(mockMySqlConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    MySqlDriver driver = new MySqlDriver() {
      @Override
      protected Connection createMySqlConnection(String url, String user, String password) {
        return mockMySqlConnection;
      }

      @Override
      protected TableSchema createTableSchema(Properties properties, String schemaName) {
        return mockTableSchema;
      }
    };

    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");
    Connection connection = driver.connect("jdbc:mysql-hoptimator://", props);
    assertNotNull(connection);
    // setAutoCommit(true) was called — verify the resulting connection has autocommit=true
    assertTrue(connection.getAutoCommit(),
        "Connection should have autoCommit=true after connect()");
    connection.close();
  }

  // --- connect() called twice succeeds ---

  @Test
  void connectCalledTwiceSucceeds() throws SQLException {
    when(mockMySqlConnection.getMetaData()).thenReturn(mockMetaData);
    when(mockMetaData.getCatalogs()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    MySqlDriver driver = new MySqlDriver() {
      @Override
      protected Connection createMySqlConnection(String url, String user, String password) {
        return mockMySqlConnection;
      }

      @Override
      protected TableSchema createTableSchema(Properties properties, String schemaName) {
        return mockTableSchema;
      }
    };

    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");

    Connection c1 = driver.connect("jdbc:mysql-hoptimator://", props);
    assertNotNull(c1);
    c1.close();

    // Reset the resultset mock for the second call
    when(mockResultSet.next()).thenReturn(false);
    Connection c2 = driver.connect("jdbc:mysql-hoptimator://", props);
    assertNotNull(c2);
    c2.close();
  }

  // --- connect() with null url in props: already tested via connectWithMissingUrlThrowsSQLException ---
  // --- Additional: connect with wrong prefix returns null ---

  @Test
  void connectReturnsNullForNonMatchingUrl() throws SQLException {
    MySqlDriver driver = new MySqlDriver();
    assertNull(driver.connect("jdbc:other://localhost/db", new Properties()));
  }
}
