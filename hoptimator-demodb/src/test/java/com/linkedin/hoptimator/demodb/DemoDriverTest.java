package com.linkedin.hoptimator.demodb;

import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class DemoDriverTest {

  @Test
  void testGetConnectStringPrefix() {
    DemoDriver driver = new DemoDriver();
    assertEquals("jdbc:demodb://", driver.getConnectStringPrefix());
  }

  @Test
  void testCreateDriverVersion() {
    DemoDriver driver = new DemoDriver();
    assertNotNull(driver.createDriverVersion());
  }

  @Test
  void testConnectWithWrongPrefixReturnsNull() throws SQLException {
    DemoDriver driver = new DemoDriver();
    Properties props = new Properties();
    assertNull(driver.connect("jdbc:other://localhost", props));
  }

  @Test
  void connectWithAllSchemasRegistered() throws SQLException {
    DemoDriver driver = new DemoDriver();
    Properties props = new Properties();
    props.setProperty("names", "");
    Connection connection = driver.connect("jdbc:demodb://;names=", props);
    assertNotNull(connection);
    assertTrue(connection instanceof CalciteConnection);
    assertTrue(connection.getAutoCommit());
    CalciteConnection calciteConnection = (CalciteConnection) connection;
    assertNotNull(calciteConnection.getRootSchema().subSchemas().get("PROFILE"));
    assertNotNull(calciteConnection.getRootSchema().subSchemas().get("ADS"));
    connection.close();
  }

  @Test
  void connectWithSpecificSchemaRegistersOnlyThatSchema() throws SQLException {
    DemoDriver driver = new DemoDriver();
    Properties props = new Properties();
    props.setProperty("names", "PROFILE");
    Connection connection = driver.connect("jdbc:demodb://;names=PROFILE", props);
    assertNotNull(connection);
    CalciteConnection calciteConnection = (CalciteConnection) connection;
    assertNotNull(calciteConnection.getRootSchema().subSchemas().get("PROFILE"));
    assertNull(calciteConnection.getRootSchema().subSchemas().get("ADS"));
    connection.close();
  }

  @Test
  void connectWithAdsSchemaOnly() throws SQLException {
    DemoDriver driver = new DemoDriver();
    Properties props = new Properties();
    props.setProperty("names", "ADS");
    Connection connection = driver.connect("jdbc:demodb://;names=ADS", props);
    assertNotNull(connection);
    CalciteConnection calciteConnection = (CalciteConnection) connection;
    assertNull(calciteConnection.getRootSchema().subSchemas().get("PROFILE"));
    assertNotNull(calciteConnection.getRootSchema().subSchemas().get("ADS"));
    connection.close();
  }

  @Test
  void connectHandlesCaseInsensitiveNames() throws SQLException {
    DemoDriver driver = new DemoDriver();
    Properties props = new Properties();
    props.setProperty("names", "profile");
    Connection connection = driver.connect("jdbc:demodb://;names=profile", props);
    assertNotNull(connection);
    CalciteConnection calciteConnection = (CalciteConnection) connection;
    assertNotNull(calciteConnection.getRootSchema().subSchemas().get("PROFILE"));
    connection.close();
  }

  @Test
  void connectWithMissingNamesPropertyThrowsException() {
    DemoDriver driver = new DemoDriver();
    Properties props = new Properties();
    assertThrows(Exception.class,
        () -> driver.connect("jdbc:demodb://", props));
  }
}
