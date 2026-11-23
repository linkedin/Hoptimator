package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


class CalciteDriverTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void successfulInitializationRunsHooksInOrderWithoutClosing(boolean cache) throws SQLException {
    List<String> events = new ArrayList<>();
    AtomicInteger closes = new AtomicInteger();
    Properties input = new Properties();
    input.setProperty("setting", "caller");
    input.setProperty("callerOnly", "retained");
    CalciteDriver driver = new CalciteDriver() {
      @Override protected void onConnectionInit(AvaticaConnection connection) throws SQLException {
        super.onConnectionInit(connection);
        events.add("calcite");
      }

      @Override protected void configureConnection(CalciteConnection connection, String url, Properties properties)
          throws SQLException {
        events.add("driver");
        assertEquals("url", properties.getProperty("setting"));
        assertEquals("retained", properties.getProperty("callerOnly"));
        addConnectionResource(connection, closes::incrementAndGet);
      }
    };

    try (Connection result = driver.connect("jdbc:calcite:setting=url", input, cache)) {
      assertEquals(List.of("calcite", "driver"), events);
      assertFalse(result.isClosed());
      assertEquals(0, closes.get());
      assertEquals("caller", input.getProperty("setting"));
    }
    assertEquals(1, closes.get());
  }

  @ParameterizedTest
  @MethodSource("initializationFailures")
  void failedInitializationClosesConnectionAndPreservesFailure(Exception expected) throws Exception {
    AtomicReference<CalciteConnection> created = new AtomicReference<>();
    AtomicInteger closes = new AtomicInteger();
    CalciteDriver driver = new CalciteDriver() {
      @Override protected void configureConnection(CalciteConnection connection, String url, Properties properties)
          throws Exception {
        created.set(connection);
        addConnectionResource(connection, closes::incrementAndGet);
        throw expected;
      }
    };

    Exception failure = assertThrows(Exception.class, () -> driver.connect("jdbc:calcite:", new Properties()));

    if (expected instanceof IOException) {
      assertEquals(SQLException.class, failure.getClass());
      assertSame(expected, failure.getCause());
    } else {
      assertSame(expected, failure);
    }
    assertTrue(created.get().isClosed());
    assertEquals(1, closes.get());
  }

  @Test
  void failedInitializationPreservesFailureDespiteCleanupFailure() throws SQLException {
    SQLException expected = new SQLException("initialization failed");
    SQLException cleanupFailure = new SQLException("close failed");
    AtomicReference<CalciteConnection> created = new AtomicReference<>();
    CalciteDriver driver = new CalciteDriver() {
      @Override protected void configureConnection(CalciteConnection connection, String url, Properties properties)
          throws SQLException {
        created.set(connection);
        addConnectionResource(connection, () -> {
          throw cleanupFailure;
        });
        throw expected;
      }

      @Override protected SQLException connectionException(String url, Exception failure) {
        try {
          assertTrue(created.get().isClosed());
        } catch (SQLException e) {
          throw new AssertionError(e);
        }
        return super.connectionException(url, failure);
      }
    };

    SQLException failure = assertThrows(SQLException.class,
        () -> driver.connect("jdbc:calcite:", new Properties()));

    assertSame(expected, failure);
    assertEquals(0, failure.getSuppressed().length);
  }

  @Test
  void successfulOperationKeepsResultAndClosesAllResourcesDespiteCleanupFailure() throws SQLException {
    List<Integer> closes = new ArrayList<>();
    CalciteConnection connection = (CalciteConnection) new CalciteDriver()
        .connect("jdbc:calcite:", new Properties());
    CalciteDriver.addConnectionResource(connection, () -> closes.add(1));
    CalciteDriver.addConnectionResource(connection, () -> {
      closes.add(2);
      throw new IOException("cleanup failed");
    });
    CalciteDriver.addConnectionResource(connection, () -> {
      closes.add(3);
      throw new IllegalStateException("another cleanup failure");
    });

    String result;
    try (connection) {
      result = "completed";
    }
    assertNotNull(connection);
    connection.close();

    assertEquals("completed", result);
    assertTrue(connection.isClosed());
    assertEquals(List.of(3, 2, 1), closes);
  }

  @ParameterizedTest
  @MethodSource("initializationFailures")
  void operationFailureIsNotReplacedByCleanupFailure(Exception expected) throws SQLException {
    AtomicInteger closes = new AtomicInteger();
    Connection connection = new CalciteDriver().connect("jdbc:calcite:", new Properties());
    CalciteDriver.addConnectionResource((CalciteConnection) connection, () -> {
      closes.incrementAndGet();
      throw new IOException("cleanup failed");
    });

    Exception failure = assertThrows(Exception.class, () -> {
      try (connection) {
        throw expected;
      }
    });

    assertSame(expected, failure);
    assertEquals(0, failure.getSuppressed().length);
    assertTrue(connection.isClosed());
    assertEquals(1, closes.get());
  }

  private static Stream<Exception> initializationFailures() {
    return Stream.of(new SQLException("SQL setup failure"), new IOException("I/O setup failure"),
        new IllegalStateException("runtime setup failure"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"calcite", "adapter"})
  void initializationErrorClosesConnectionAndPropagatesUnchanged(String stage) throws SQLException {
    AssertionError expected = new AssertionError("initialization error");
    AtomicReference<CalciteConnection> created = new AtomicReference<>();
    AtomicInteger closes = new AtomicInteger();
    CalciteDriver driver = new CalciteDriver() {
      private void fail(CalciteConnection connection) throws SQLException {
        created.set(connection);
        addConnectionResource(connection, closes::incrementAndGet);
        throw expected;
      }

      @Override protected void onConnectionInit(AvaticaConnection connection) throws SQLException {
        if ("calcite".equals(stage)) {
          fail((CalciteConnection) connection);
        }
        super.onConnectionInit(connection);
      }

      @Override protected void configureConnection(CalciteConnection connection, String url, Properties properties)
          throws SQLException {
        fail(connection);
      }
    };

    try {
      AssertionError failure = assertThrows(AssertionError.class,
          () -> driver.connect("jdbc:calcite:", new Properties()));

      assertSame(expected, failure);
      assertTrue(created.get().isClosed());
      assertEquals(1, closes.get());
    } finally {
      if (created.get() != null) {
        created.get().close();
      }
    }
  }

  @ParameterizedTest
  @MethodSource("cleanupFailures")
  void failedInitializationCleanupDoesNotReplacePrimaryFailure(Throwable cleanup) throws SQLException {
    Connection connection = mock(Connection.class);
    doThrow(cleanup).when(connection).close();
    SQLException original = new SQLException("initialization failed");

    CalciteDriver.closeOnFailure(connection, original);

    assertEquals(1, original.getSuppressed().length);
    assertSame(cleanup, original.getSuppressed()[0]);
    verify(connection).close();
  }

  private static Stream<Throwable> cleanupFailures() {
    return Stream.of(new SQLException("SQL cleanup failure"), new IllegalStateException("runtime cleanup failure"),
        new AssertionError("cleanup error"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void initializationFailureClosesConnectionAndOwnedResources(boolean cache) throws SQLException {
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
        () -> driver.connect("jdbc:calcite:", new Properties(), cache));

    assertSame(expected, failure);
    assertTrue(created.get().isClosed());
    assertTrue(closed.get());
  }

  @Test
  void validationFailurePrecedesAcquisitionAndExceptionMapping() {
    SQLException expected = new SQLException("invalid properties");
    AtomicBoolean rootCreated = new AtomicBoolean();
    AtomicBoolean mapped = new AtomicBoolean();
    CalciteDriver driver = new CalciteDriver() {
      @Override protected Properties connectionProperties(String url, Properties info) throws SQLException {
        throw expected;
      }

      @Override protected CalciteSchema createRootSchema(Properties properties) {
        rootCreated.set(true);
        return super.createRootSchema(properties);
      }

      @Override protected SQLException connectionException(String url, Exception failure) {
        mapped.set(true);
        return super.connectionException(url, failure);
      }
    };

    assertSame(expected, assertThrows(SQLException.class, () -> driver.connect("jdbc:calcite:", new Properties())));
    assertFalse(rootCreated.get());
    assertFalse(mapped.get());
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
