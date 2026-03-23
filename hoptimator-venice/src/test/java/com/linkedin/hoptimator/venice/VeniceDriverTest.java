package com.linkedin.hoptimator.venice;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class VeniceDriverTest {

  @Mock
  private ClusterSchema mockClusterSchema;

  @Test
  void testGetConnectStringPrefix() {
    VeniceDriver driver = new VeniceDriver();
    assertEquals("jdbc:venice://", driver.getConnectStringPrefix());
  }

  @Test
  void testCreateDriverVersion() {
    VeniceDriver driver = new VeniceDriver();
    assertNotNull(driver.createDriverVersion());
  }

  @Test
  void testConnectWithWrongPrefixReturnsNull() throws SQLException {
    VeniceDriver driver = new VeniceDriver();
    Properties props = new Properties();
    assertNull(driver.connect("jdbc:other://localhost", props));
  }

  @Test
  void testCreateClusterSchema() {
    VeniceDriver driver = new VeniceDriver();
    Properties props = new Properties();
    props.setProperty("router.url", "http://localhost:1234");
    ClusterSchema schema = driver.createClusterSchema(props);
    assertNotNull(schema);
  }

  @Test
  void connectWithCorrectPrefixReturnsConnection() throws SQLException {
    VeniceDriver driver = new VeniceDriver() {
      @Override
      protected ClusterSchema createClusterSchema(Properties properties) {
        return mockClusterSchema;
      }
    };
    Connection connection = driver.connect("jdbc:venice://", new Properties());
    assertNotNull(connection);
    assertTrue(connection instanceof CalciteConnection);
    assertTrue(connection.getAutoCommit());
    assertEquals("VENICE", connection.getCatalog());
    connection.close();
  }

  @Test
  void connectRegistersVeniceSchema() throws SQLException {
    VeniceDriver driver = new VeniceDriver() {
      @Override
      protected ClusterSchema createClusterSchema(Properties properties) {
        return mockClusterSchema;
      }
    };
    Connection connection = driver.connect("jdbc:venice://", new Properties());
    CalciteConnection calciteConnection = (CalciteConnection) connection;
    assertNotNull(calciteConnection.getRootSchema().subSchemas().get("VENICE"));
    connection.close();
  }

  @Test
  void connectWithExceptionWrapsInSQLNonTransientException() {
    VeniceDriver driver = new VeniceDriver() {
      @Override
      protected ClusterSchema createClusterSchema(Properties properties) {
        throw new RuntimeException("test error");
      }
    };
    SQLNonTransientException exception = assertThrows(SQLNonTransientException.class,
        () -> driver.connect("jdbc:venice://", new Properties()));
    assertTrue(exception.getMessage().contains("Problem loading"));
  }
}
