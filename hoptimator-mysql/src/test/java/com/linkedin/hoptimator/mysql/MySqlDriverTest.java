package com.linkedin.hoptimator.mysql;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.IgnoreCaseLookup;
import org.apache.calcite.schema.lookup.LikePattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

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

  /** Returns a driver whose {@code createMySqlRootSchema()} yields a no-op schema. */
  private MySqlDriver driverWithMockRootSchema() {
    return new MySqlDriver() {
      @Override
      protected AbstractSchema createMySqlRootSchema(Properties properties) {
        return new AbstractSchema() {
          @Override
          public org.apache.calcite.schema.lookup.Lookup<Schema> subSchemas() {
            return new IgnoreCaseLookup<>() {
              @Override public Schema get(String name) {
                return null;
              }
              @Override public Set<String> getNames(LikePattern pattern) {
                return Collections.emptySet();
              }
            };
          }
        };
      }
    };
  }

  @Test
  void testGetConnectStringPrefix() {
    assertEquals("jdbc:mysql-hoptimator://", new MySqlDriver().getConnectStringPrefix());
  }

  @Test
  void testCreateDriverVersion() {
    assertNotNull(new MySqlDriver().createDriverVersion());
  }

  @Test
  void testConnectWithWrongPrefixReturnsNull() throws SQLException {
    assertNull(new MySqlDriver().connect("jdbc:other://localhost", new Properties()));
  }

  @Test
  void connectWithCorrectPrefixReturnsConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");
    Connection connection = driverWithMockRootSchema().connect("jdbc:mysql-hoptimator://", props);

    assertNotNull(connection);
    assertInstanceOf(CalciteConnection.class, connection);
    assertTrue(connection.getAutoCommit());
    assertEquals("MYSQL", connection.getCatalog());
    connection.close();
  }

  @Test
  void connectWithMissingUrlThrowsSQLException() {
    SQLException exception = assertThrows(SQLException.class,
        () -> driverWithMockRootSchema().connect("jdbc:mysql-hoptimator://", new Properties()));
    assertTrue(exception.getMessage().contains("Missing required parameter 'url'"));
  }

  @Test
  void connectCalledTwiceSucceeds() throws SQLException {
    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");
    MySqlDriver driver = driverWithMockRootSchema();

    Connection c1 = driver.connect("jdbc:mysql-hoptimator://", props);
    assertNotNull(c1);
    c1.close();

    Connection c2 = driver.connect("jdbc:mysql-hoptimator://", props);
    assertNotNull(c2);
    c2.close();
  }

  @Test
  void connectReturnsNullForNonMatchingUrl() throws SQLException {
    assertNull(new MySqlDriver().connect("jdbc:other://localhost/db", new Properties()));
  }

  @Test
  void createMySqlRootSchemaReturnsNonNull() {
    Properties props = new Properties();
    props.setProperty("url", "jdbc:mysql://localhost:3306");
    assertNotNull(new MySqlDriver().createMySqlRootSchema(props));
  }
}
