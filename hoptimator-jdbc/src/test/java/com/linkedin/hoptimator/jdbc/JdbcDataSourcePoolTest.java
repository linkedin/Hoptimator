package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.ref.Reference;
import java.nio.file.Files;
import java.nio.file.Path;
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

import static org.assertj.core.api.Assertions.assertThat;
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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void gcClosesUnleasedPoolsAndKeepsActiveCatalogsUsable(boolean failFirstClose, @TempDir Path temporary)
      throws Exception {
    Path output = temporary.resolve("pool-gc.log");
    Process process = new ProcessBuilder(
        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Xmx64m", "-cp",
        System.getProperty("hoptimator.test.classpath", System.getProperty("java.class.path")),
        GcProbe.class.getName(), Boolean.toString(failFirstClose))
        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      assertTrue(process.waitFor(90, TimeUnit.SECONDS), "Isolated GC probe must finish");
      String log = Files.readString(output);
      assertEquals(0, process.exitValue(), log);
      assertThat(log).contains("reclaimed=3; created=4; failures=" + (failFirstClose ? 1 : 0) + "; open=0");
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "GC probe must terminate");
      }
    }
  }

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
      @Override protected void configureConnection(CalciteConnection connection, String url, Properties properties)
          throws SQLException {
        connection.setAutoCommit(true);
        addConnectionResource(connection, physicalCloses::incrementAndGet);
      }
    };
    return new JdbcDataSourcePool(() -> {
      BasicDataSource source = new BasicDataSource();
      source.setDriver(driver);
      return source;
    });
  }

  /** Heap pressure is confined to this small child JVM, never the shared test worker. */
  public static final class GcProbe {
    private static final int MIB = 1024 * 1024;
    private static final String ACTIVE_URL = URL + "pool=active";
    private static final String IDLE_URL = URL + "pool=idle";

    public static void main(String[] args) throws Exception {
      boolean failFirstClose = Boolean.parseBoolean(args[0]);
      AtomicInteger createdPools = new AtomicInteger();
      AtomicInteger opened = new AtomicInteger();
      AtomicInteger closed = new AtomicInteger();
      AtomicInteger failures = new AtomicInteger();
      CalciteDriver driver = new CalciteDriver() {
        @Override protected void configureConnection(CalciteConnection connection, String url, Properties properties)
            throws SQLException {
          connection.setAutoCommit(true);
          opened.incrementAndGet();
          addConnectionResource(connection, closed::incrementAndGet);
        }
      };
      try (JdbcDataSourcePool owner = new JdbcDataSourcePool(() -> {
        boolean fail = createdPools.incrementAndGet() == 2 && failFirstClose;
        BasicDataSource source = new BasicDataSource() {
          @Override public synchronized void close() throws SQLException {
            super.close();
            if (fail) {
              failures.incrementAndGet();
              throw new SQLException("GC cleanup failure control");
            }
          }
        };
        source.setDriver(driver);
        return source;
      });
          JdbcDataSourcePool.Lease active = owner.acquire(ACTIVE_URL, null, null, null)) {
        BasicDataSource activeSource = active.dataSource();
        CalciteConnection activePhysical;
        try (Connection connection = activeSource.getConnection()) {
          activePhysical = connection.unwrap(CalciteConnection.class);
        }
        // Even a retained closed handle must release its strong ownership token.
        List<JdbcDataSourcePool.Lease> released = new ArrayList<>();
        for (int cycle = 0; cycle < 3; cycle++) {
          released.add(warmAndRelease(owner));
          int expectedCloses = closed.get() + 1;
          awaitReclamation(closed, expectedCloses);
          assertEquals(expectedCloses, closed.get());
          assertFalse(activeSource.isClosed());
          assertFalse(activePhysical.isClosed());
          try (JdbcDataSourcePool.Lease same = owner.acquire(ACTIVE_URL, null, null, null);
              Connection connection = same.dataSource().getConnection()) {
            assertSame(activeSource, same.dataSource());
            assertSame(activePhysical, connection.unwrap(CalciteConnection.class));
          }
        }
        assertEquals(4, createdPools.get());
        assertEquals(failFirstClose ? 1 : 0, failures.get());
        Reference.reachabilityFence(released);
      }
      assertEquals(opened.get(), closed.get());
      System.out.println("reclaimed=3; created=" + createdPools.get()
          + "; failures=" + failures.get() + "; open=" + (opened.get() - closed.get()));
    }

    private static JdbcDataSourcePool.Lease warmAndRelease(JdbcDataSourcePool owner) throws SQLException {
      JdbcDataSourcePool.Lease lease = owner.acquire(IDLE_URL, null, null, null);
      try (Connection connection = lease.dataSource().getConnection()) {
        assertTrue(connection.getAutoCommit());
      }
      lease.close();
      return lease;
    }

    private static void awaitReclamation(AtomicInteger closed, int expected) throws InterruptedException {
      List<byte[]> pressure = new ArrayList<>();
      Runtime runtime = Runtime.getRuntime();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
      do {
        while (runtime.maxMemory() - runtime.totalMemory() + runtime.freeMemory() > 8L * MIB
            && closed.get() < expected) {
          pressure.add(new byte[16 * 1024]);
        }
        System.gc();
        if (closed.get() >= expected) {
          break;
        }
        TimeUnit.MILLISECONDS.sleep(100);
      } while (System.nanoTime() < deadline);
      Reference.reachabilityFence(pressure);
      assertEquals(expected, closed.get(), "GC must dispose the idle physical connection without invalidation");
    }
  }

  private static final class CountingDataSource extends BasicDataSource {
    private final AtomicInteger closes = new AtomicInteger();

    @Override public synchronized void close() throws SQLException {
      closes.incrementAndGet();
      super.close();
    }
  }
}
