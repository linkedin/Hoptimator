package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.jdbc.HoptimatorCalciteSchema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;


@ExtendWith(MockitoExtension.class)
class VeniceDriverTest {

  @Mock
  private ClusterSchema mockClusterSchema;

  @Test
  void pooledReturnKeepsSchemaOpenUntilPhysicalDisposal() throws Exception {
    List<ClusterSchema> created = new ArrayList<>();
    VeniceDriver driver = new VeniceDriver() {
      @Override protected ClusterSchema createClusterSchema(Properties properties) {
        ClusterSchema schema = mock(ClusterSchema.class, withSettings().extraInterfaces(AutoCloseable.class));
        created.add(schema);
        return schema;
      }
    };
    try (BasicDataSource pool = new BasicDataSource()) {
      pool.setDriver(driver);
      pool.setUrl(VeniceDriver.CONNECTION_PREFIX);
      pool.setMaxTotal(1);
      Connection lease = pool.getConnection();
      AutoCloseable owned = (AutoCloseable) created.get(created.size() - 1);

      lease.close();
      assertTrue(lease.isClosed());
      verify(owned, never()).close();
      try (Connection reused = pool.getConnection()) {
        assertEquals("VENICE", reused.getCatalog());
        verify(owned, never()).close();
      }
      pool.close();
      pool.close();

      verify(owned).close();
    }
    for (ClusterSchema schema : created) {
      verify((AutoCloseable) schema).close();
    }
  }

  @Test
  void schemaConstructionFailureClosesCreatedConnection() throws SQLException {
    AtomicReference<AvaticaConnection> created = new AtomicReference<>();
    RuntimeException expected = new IllegalStateException("schema construction failed");
    VeniceDriver driver = new VeniceDriver() {
      @Override protected void onConnectionInit(AvaticaConnection connection) throws SQLException {
        created.set(connection);
        super.onConnectionInit(connection);
      }

      @Override protected ClusterSchema createClusterSchema(Properties properties) {
        throw expected;
      }
    };

    SQLException failure = assertThrows(SQLException.class,
        () -> driver.connect(VeniceDriver.CONNECTION_PREFIX, new Properties()));

    assertSame(expected, failure.getCause());
    assertTrue(created.get().isClosed());
  }

  @Test
  void registrationFailureClosesSchemaAndPreservesOriginalError() throws Exception {
    ClusterSchema owned = mock(ClusterSchema.class, withSettings().extraInterfaces(AutoCloseable.class));
    RuntimeException expected = new IllegalStateException("registration failed");
    VeniceDriver driver = new VeniceDriver() {
      @Override protected CalciteSchema createRootSchema(Properties properties) {
        return new HoptimatorCalciteSchema(null, new AbstractSchema(), "") {
          @Override public CalciteSchema add(String name, Schema schema) {
            if (CATALOG_NAME.equals(name)) {
              throw expected;
            }
            return super.add(name, schema);
          }
        };
      }

      @Override protected ClusterSchema createClusterSchema(Properties properties) {
        return owned;
      }
    };

    SQLException failure = assertThrows(SQLException.class,
        () -> driver.connect(VeniceDriver.CONNECTION_PREFIX, new Properties()));

    assertSame(expected, failure.getCause());
    verify((AutoCloseable) owned).close();
  }

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
    assertInstanceOf(CalciteConnection.class, connection);
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

  @Test
  void connectTwiceToSameUrlSucceeds() throws SQLException {
    // connect twice succeeds (super.connect returns non-null both times)
    VeniceDriver driver = new VeniceDriver() {
      @Override
      protected ClusterSchema createClusterSchema(Properties properties) {
        return mockClusterSchema;
      }
    };
    Connection first = driver.connect("jdbc:venice://", new Properties());
    assertNotNull(first);
    Connection second = driver.connect("jdbc:venice://", new Properties());
    assertNotNull(second);
    assertEquals("VENICE", first.getCatalog());
    assertEquals("VENICE", second.getCatalog());
    first.close();
    second.close();
  }

}
