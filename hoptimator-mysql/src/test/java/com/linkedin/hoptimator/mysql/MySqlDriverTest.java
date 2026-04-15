package com.linkedin.hoptimator.mysql;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE", "DMI_EMPTY_DB_PASSWORD"},
    justification = "Mock objects do not hold real resources")
class MySqlDriverTest {

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
    MySqlDriver driver = new MySqlDriver();

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
  void connectRegistersMysqlCatalogSchemaInRootSchema() throws SQLException {
    MySqlDriver driver = new MySqlDriver();

    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");
    Connection connection = driver.connect("jdbc:mysql-hoptimator://", props);
    CalciteConnection calciteConnection = (CalciteConnection) connection;

    // Calcite wraps registered schemas in SchemaPlusImpl; just verify MYSQL is present.
    // The absence of any MySQL connection mock here also proves no connection is opened eagerly.
    Schema mysqlSchema = calciteConnection.getRootSchema().subSchemas().get("MYSQL");
    assertNotNull(mysqlSchema);
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
  void connectCalledTwiceSucceeds() throws SQLException {
    MySqlDriver driver = new MySqlDriver();

    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");

    Connection c1 = driver.connect("jdbc:mysql-hoptimator://", props);
    assertNotNull(c1);
    c1.close();

    Connection c2 = driver.connect("jdbc:mysql-hoptimator://", props);
    assertNotNull(c2);
    c2.close();
  }

  @Test
  void connectReturnsNullForNonMatchingUrl() throws SQLException {
    MySqlDriver driver = new MySqlDriver();
    assertNull(driver.connect("jdbc:other://localhost/db", new Properties()));
  }

  @Test
  void createMySqlCatalogSchemaReturnsNonNull() {
    MySqlDriver driver = new MySqlDriver();
    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");
    MySqlCatalogSchema schema = driver.createMySqlCatalogSchema(props);
    assertNotNull(schema);
  }
}
