package com.linkedin.hoptimator.kafka;

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
class KafkaDriverTest {

  @Mock
  private ClusterSchema mockClusterSchema;

  @Test
  void testGetConnectStringPrefix() {
    KafkaDriver driver = new KafkaDriver();
    assertEquals("jdbc:kafka://", driver.getConnectStringPrefix());
  }

  @Test
  void testCreateDriverVersion() {
    KafkaDriver driver = new KafkaDriver();
    assertNotNull(driver.createDriverVersion());
  }

  @Test
  void testConnectWithWrongPrefixReturnsNull() throws SQLException {
    KafkaDriver driver = new KafkaDriver();
    Properties props = new Properties();
    assertNull(driver.connect("jdbc:other://localhost", props));
  }

  @Test
  void connectWithCorrectPrefixReturnsConnection() throws SQLException {
    KafkaDriver driver = new KafkaDriver() {
      @Override
      protected ClusterSchema createClusterSchema(Properties properties) {
        return mockClusterSchema;
      }
    };
    Connection connection = driver.connect("jdbc:kafka://", new Properties());
    assertNotNull(connection);
    assertTrue(connection instanceof CalciteConnection);
    assertTrue(connection.getAutoCommit());
    connection.close();
  }

  @Test
  void connectRegistersKafkaSchema() throws SQLException {
    KafkaDriver driver = new KafkaDriver() {
      @Override
      protected ClusterSchema createClusterSchema(Properties properties) {
        return mockClusterSchema;
      }
    };
    Connection connection = driver.connect("jdbc:kafka://", new Properties());
    CalciteConnection calciteConnection = (CalciteConnection) connection;
    assertNotNull(calciteConnection.getRootSchema().getSubSchema("KAFKA"));
    connection.close();
  }

  @Test
  void connectWithExceptionWrapsInSQLNonTransientException() {
    KafkaDriver driver = new KafkaDriver() {
      @Override
      protected ClusterSchema createClusterSchema(Properties properties) {
        throw new RuntimeException("test error");
      }
    };
    SQLNonTransientException exception = assertThrows(SQLNonTransientException.class,
        () -> driver.connect("jdbc:kafka://", new Properties()));
    assertTrue(exception.getMessage().contains("Problem loading"));
  }

  @Test
  void createClusterSchemaReturnsInstance() {
    KafkaDriver driver = new KafkaDriver();
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    ClusterSchema schema = driver.createClusterSchema(props);
    assertNotNull(schema);
  }
}
