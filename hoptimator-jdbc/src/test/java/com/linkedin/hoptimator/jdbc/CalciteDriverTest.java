package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
class CalciteDriverTest {
  @Mock private Connection connection;
  @Mock private Connection initialized;

  @Test
  void successfulInitializationTransfersOwnershipWithoutClosing() throws SQLException {
    Connection result = CalciteDriver.initializeConnection(connection, () -> initialized);

    assertSame(initialized, result);
    verify(connection, never()).close();
  }

  @ParameterizedTest
  @MethodSource("initializationFailures")
  void failedInitializationClosesConnectionAndPreservesFailure(Exception expected) throws Exception {
    Exception failure = assertThrows(expected.getClass(), () ->
        CalciteDriver.initializeConnection(connection, () -> {
          throw expected;
        }));

    assertSame(expected, failure);
    verify(connection).close();
  }

  @Test
  void failedInitializationSuppressesCleanupFailure() throws SQLException {
    SQLException expected = new SQLException("initialization failed");
    SQLException cleanupFailure = new SQLException("close failed");
    doThrow(cleanupFailure).when(connection).close();

    SQLException failure = assertThrows(SQLException.class, () ->
        CalciteDriver.initializeConnection(connection, () -> {
          throw expected;
        }));

    assertSame(expected, failure);
    assertArrayEquals(new Throwable[]{cleanupFailure}, failure.getSuppressed());
    verify(connection).close();
  }

  private static Stream<Exception> initializationFailures() {
    return Stream.of(new SQLException("SQL setup failure"), new IOException("I/O setup failure"),
        new IllegalStateException("runtime setup failure"));
  }

  @Test
  void initializationFailureClosesConnectionAndOwnedResources() throws SQLException {
    AtomicReference<AvaticaConnection> created = new AtomicReference<>();
    AtomicBoolean closed = new AtomicBoolean();
    SQLException expected = new SQLException("initialization failed");
    CalciteDriver driver = new CalciteDriver() {
      @Override protected void onConnectionInit(AvaticaConnection connection) throws SQLException {
        created.set(connection);
        addConnectionResource((CalciteConnection) connection, () -> closed.set(true));
        throw expected;
      }
    };

    SQLException failure = assertThrows(SQLException.class,
        () -> driver.connect("jdbc:calcite:", new Properties()));

    assertSame(expected, failure);
    assertTrue(created.get().isClosed());
    assertTrue(closed.get());
  }

  @Test
  void closesOnlyExplicitlyOwnedResources() throws SQLException {
    AtomicBoolean ownedClosed = new AtomicBoolean();
    AtomicBoolean borrowedClosed = new AtomicBoolean();
    class BorrowedSchema extends AbstractSchema implements AutoCloseable {
      @Override public void close() {
        borrowedClosed.set(true);
      }
    }
    CalciteConnection connection = (CalciteConnection) new CalciteDriver()
        .connect("jdbc:calcite:", new Properties());
    connection.getRootSchema().add("borrowed", new BorrowedSchema());
    CalciteDriver.addConnectionResource(connection, () -> ownedClosed.set(true));

    connection.close();
    connection.close();

    assertTrue(ownedClosed.get());
    assertFalse(borrowedClosed.get());
  }

  @Test
  void testDefaultConstructorCreatesDriver() {
    CalciteDriver driver = new CalciteDriver();

    assertNotNull(driver);
  }

  @Test
  void testConnectReturnsNullForNonCalciteUrl() throws SQLException {
    CalciteDriver driver = new CalciteDriver();

    Connection result = driver.connect("jdbc:mysql://localhost", new Properties());

    assertNull(result);
  }

  @Test
  void testConnectWithCalciteUrl() throws SQLException {
    CalciteDriver driver = new CalciteDriver();
    Properties props = new Properties();

    Connection result = driver.connect("jdbc:calcite:", props);

    assertNotNull(result);
    result.close();
  }

  @Test
  void testConnectWithCacheDisabled() throws SQLException {
    CalciteDriver driver = new CalciteDriver();
    Properties props = new Properties();

    Connection result = driver.connect("jdbc:calcite:", props, false);

    assertNotNull(result);
    result.close();
  }

  @Test
  void testConnectWithCacheEnabled() throws SQLException {
    CalciteDriver driver = new CalciteDriver();
    Properties props = new Properties();

    Connection result = driver.connect("jdbc:calcite:", props, true);

    assertNotNull(result);
    result.close();
  }

  @Test
  void testWithPrepareFactoryReturnsSameIfSameFactory() {
    CalciteDriver driver = new CalciteDriver();

    // The internal prepareFactory is null for default constructor,
    // so we need to set one first, then set the same one
    CalciteDriver driver2 = driver.withPrepareFactory(() -> null);
    CalciteDriver driver3 = driver2.withPrepareFactory(() -> null);

    // Different lambda instances, so should create a new driver
    assertNotSame(driver2, driver3);
  }

  @Test
  void testWithPrepareFactoryReturnsSameInstanceWhenSameFactory() {
    Supplier<CalcitePrepare> factory = () -> null;
    CalciteDriver driver = new CalciteDriver();
    CalciteDriver driver2 = driver.withPrepareFactory(factory);
    CalciteDriver driver3 = driver2.withPrepareFactory(factory);

    assertSame(driver2, driver3);
  }

  @Test
  void testConnectWithCacheReturnNullForWrongUrl() throws SQLException {
    CalciteDriver driver = new CalciteDriver();

    Connection result = driver.connect("jdbc:mysql://localhost", new Properties(), false);

    assertNull(result);
  }
}
