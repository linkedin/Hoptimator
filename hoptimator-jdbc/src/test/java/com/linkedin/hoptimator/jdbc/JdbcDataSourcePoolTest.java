package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


class JdbcDataSourcePoolTest {
  private static final String URL = "jdbc:calcite:";

  @Test
  void reusesPhysicalConnectionAcrossCatalogLeasesAndClosesOnRemoval() throws Exception {
    AtomicInteger physicalCloses = new AtomicInteger();
    try (JdbcDataSourcePool owner = realPool(physicalCloses)) {
      JdbcDataSourcePool.Lease first = owner.acquire(URL, null, null, null);
      BasicDataSource source = first.dataSource();
      CalciteConnection physical;
      try (Connection connection = source.getConnection()) {
        physical = connection.unwrap(CalciteConnection.class);
      }
      int initialCloses = physicalCloses.get();
      first.close();
      assertEquals(initialCloses, physicalCloses.get());
      assertFalse(physical.isClosed());

      try (JdbcDataSourcePool.Lease second = owner.acquire(URL, null, null, null);
          Connection connection = second.dataSource().getConnection()) {
        assertSame(source, second.dataSource());
        assertSame(physical, connection.unwrap(CalciteConnection.class));
      }
      owner.invalidate(URL, null, null, null);
      owner.invalidate(URL, null, null, null);

      assertTrue(source.isClosed());
      assertTrue(physical.isClosed());
      assertEquals(initialCloses + 1, physicalCloses.get());
      assertThrows(IllegalStateException.class, first::dataSource);
    }
  }

  @Test
  void retirementPreservesLazyCatalogReadsAndSeparatesReplacementPool() throws Exception {
    AtomicInteger physicalCloses = new AtomicInteger();
    try (JdbcDataSourcePool owner = realPool(physicalCloses);
        JdbcDataSourcePool.Lease first = owner.acquire(URL, null, null, null);
        JdbcDataSourcePool.Lease second = owner.acquire(URL, null, null, null)) {
      BasicDataSource retired = first.dataSource();
      owner.invalidateAll();
      first.close();
      try (Connection connection = second.dataSource().getConnection()) {
        assertFalse(connection.isClosed());
        assertFalse(retired.isClosed());
      }
      int initialCloses = physicalCloses.get();
      try (JdbcDataSourcePool.Lease replacement = owner.acquire(URL, null, null, null)) {
        assertNotSame(retired, replacement.dataSource());
        second.close();
        second.close();
        assertTrue(retired.isClosed());
        assertFalse(replacement.dataSource().isClosed());
      }
      assertEquals(initialCloses + 1, physicalCloses.get());
    }
  }

  @Test
  void shutdownRejectsAcquisitionAndDrainsActiveCatalogBeforePhysicalClose() throws Exception {
    AtomicInteger physicalCloses = new AtomicInteger();
    try (JdbcDataSourcePool owner = realPool(physicalCloses);
        JdbcDataSourcePool.Lease lease = owner.acquire(URL, null, null, null)) {
      BasicDataSource source = lease.dataSource();
      owner.close();
      owner.close();
      assertThrows(SQLException.class, () -> owner.acquire(URL, null, null, null));
      try (Connection connection = source.getConnection()) {
        assertFalse(connection.isClosed());
      }
      int initialCloses = physicalCloses.get();
      lease.close();
      assertTrue(source.isClosed());
      assertEquals(initialCloses + 1, physicalCloses.get());
    }
  }

  @ParameterizedTest
  @CsvSource({
      "jdbc:other:,driver,user,password",
      "jdbc:calcite:,other,user,password",
      "jdbc:calcite:,driver,other,password",
      "jdbc:calcite:,driver,user,other"
  })
  void retainsExactUrlDriverAndCredentialIsolation(String url, String driver, String user, String password)
      throws Exception {
    try (JdbcDataSourcePool owner = new JdbcDataSourcePool();
        JdbcDataSourcePool.Lease first = owner.acquire(URL, "driver", "user", "password");
        JdbcDataSourcePool.Lease same = owner.acquire(URL, "driver", "user", "password");
        JdbcDataSourcePool.Lease distinct = owner.acquire(url, driver, user, password)) {
      assertSame(first.dataSource(), same.dataSource());
      assertNotSame(first.dataSource(), distinct.dataSource());
    }
  }

  @Test
  void preservesNullVersusEmptyCredentialKeys() throws Exception {
    try (JdbcDataSourcePool owner = new JdbcDataSourcePool();
        JdbcDataSourcePool.Lease unset = owner.acquire(URL, null, null, null);
        JdbcDataSourcePool.Lease empty = owner.acquire(URL, "", "", "")) {
      assertNotSame(unset.dataSource(), empty.dataSource());
    }
  }

  @Test
  void attemptsAllPoolDisposalsAndReportsEveryFailure() throws Exception {
    BasicDataSource first = mock(BasicDataSource.class);
    BasicDataSource second = mock(BasicDataSource.class);
    SQLException checked = new SQLException("first");
    IllegalStateException unchecked = new IllegalStateException("second");
    doThrow(checked).when(first).close();
    doThrow(unchecked).when(second).close();
    Queue<BasicDataSource> sources = new ArrayDeque<>(List.of(first, second));
    try (JdbcDataSourcePool owner = new JdbcDataSourcePool(sources::remove)) {
      owner.acquire("jdbc:first:", null, null, null).close();
      owner.acquire("jdbc:second:", null, null, null).close();

      SQLException failure = assertThrows(SQLException.class, owner::invalidateAll);

      List<Throwable> failures = List.of(failure.getCause(), failure.getSuppressed()[0]);
      assertEquals(1, failure.getSuppressed().length);
      assertTrue(failures.contains(checked));
      assertTrue(failures.contains(unchecked));
      owner.invalidateAll();
      verify(first, times(1)).close();
      verify(second, times(1)).close();
    }
  }

  @Test
  void finalLeaseDisposalSurfacesFailureWithoutDoubleClose() throws Exception {
    BasicDataSource source = mock(BasicDataSource.class);
    SQLException cleanup = new SQLException("cleanup");
    doThrow(cleanup).when(source).close();
    try (JdbcDataSourcePool owner = new JdbcDataSourcePool(() -> source)) {
      JdbcDataSourcePool.Lease lease = owner.acquire(URL, null, null, null);
      owner.invalidateAll();

      SQLException failure = assertThrows(SQLException.class, lease::close);

      assertSame(cleanup, failure.getCause());
      lease.close();
      verify(source, times(1)).close();
    }
  }

  @Test
  void concurrentAcquisitionRetirementAndReleaseNeverPrematurelyOrMultiplyClosePools() throws Exception {
    List<CountingDataSource> sources = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try (JdbcDataSourcePool owner = new JdbcDataSourcePool(() -> {
      CountingDataSource source = new CountingDataSource();
      sources.add(source);
      return source;
    })) {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> work = new ArrayList<>();
      for (int worker = 0; worker < 4; worker++) {
        work.add(executor.submit(() -> {
          assertTrue(start.await(10, TimeUnit.SECONDS));
          for (int iteration = 0; iteration < 40; iteration++) {
            try (JdbcDataSourcePool.Lease lease = owner.acquire(URL, null, null, null)) {
              if (iteration % 3 == 0) {
                owner.invalidateAll();
              }
              assertFalse(lease.dataSource().isClosed());
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> task : work) {
        task.get(20, TimeUnit.SECONDS);
      }
      owner.close();
      assertFalse(sources.isEmpty());
      for (CountingDataSource source : sources) {
        assertTrue(source.isClosed());
        assertEquals(1, source.closes.get());
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static JdbcDataSourcePool realPool(AtomicInteger physicalCloses) {
    CalciteDriver driver = new CalciteDriver() {
      @Override public Connection connect(String url, Properties properties) throws SQLException {
        Connection connection = super.connect(url, properties);
        connection.setAutoCommit(true);
        addConnectionResource((CalciteConnection) connection, physicalCloses::incrementAndGet);
        return connection;
      }
    };
    return new JdbcDataSourcePool(() -> {
      BasicDataSource source = new BasicDataSource();
      source.setDriver(driver);
      return source;
    });
  }

  private static final class CountingDataSource extends BasicDataSource {
    private final AtomicInteger closes = new AtomicInteger();

    @Override public synchronized void close() throws SQLException {
      closes.incrementAndGet();
      super.close();
    }
  }
}
