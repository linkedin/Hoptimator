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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
      assertThat(log).contains("reclaimed=5; created=5; failures=" + (failFirstClose ? 1 : 0) + "; open=0");
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "GC probe must terminate");
      }
    }
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  void cappedCacheUsesCleanerWithoutClosingActiveCatalogs(@TempDir Path temporary) throws Exception {
    Path output = temporary.resolve("pool-cap.log");
    Process process = new ProcessBuilder(
        Path.of(System.getProperty("java.home"), "bin", "java").toString(),
        "-Xmx64m", "-cp",
        System.getProperty("hoptimator.test.classpath", System.getProperty("java.class.path")),
        CappedProbe.class.getName())
        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      assertTrue(process.waitFor(90, TimeUnit.SECONDS), "Isolated cache-cap probe must finish");
      String log = Files.readString(output);
      assertEquals(0, process.exitValue(), log);
      assertThat(log).contains("capacity=1; created=3; reclaimed=3; active-usable=true");
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Cache-cap probe must terminate");
      }
    }
  }

  @Test
  void reusesPhysicalConnectionAfterCatalogLeaseCloses() throws Exception {
    AtomicInteger physicalCloses = new AtomicInteger();
    CalciteDriver driver = new CalciteDriver() {
      @Override protected void configureConnection(CalciteConnection connection, String url, Properties properties)
          throws SQLException {
        connection.setAutoCommit(true);
        addConnectionResource(connection, physicalCloses::incrementAndGet);
      }
    };
    JdbcDataSourcePool owner = new JdbcDataSourcePool(() -> {
      BasicDataSource source = new BasicDataSource();
      source.setDriver(driver);
      return source;
    });
    JdbcDataSourcePool.Lease first = owner.acquire(URL, null, null, null);
    BasicDataSource source = first.dataSource();
    CalciteConnection physical;
    try (Connection connection = source.getConnection()) {
      physical = connection.unwrap(CalciteConnection.class);
    }
    int initialCloses = physicalCloses.get();
    first.close();
    first.close();
    assertThrows(IllegalStateException.class, first::dataSource);
    assertEquals(initialCloses, physicalCloses.get());

    try (JdbcDataSourcePool.Lease second = owner.acquire(URL, null, null, null);
        Connection connection = second.dataSource().getConnection()) {
      assertSame(source, second.dataSource());
      assertSame(physical, connection.unwrap(CalciteConnection.class));
    } finally {
      source.close();
    }
    assertTrue(physical.isClosed());
    assertEquals(initialCloses + 1, physicalCloses.get());
  }

  @ParameterizedTest
  @CsvSource({
      "jdbc:other:,driver,user,password",
      "jdbc:calcite:,other,user,password",
      "jdbc:calcite:,driver,other,password",
      "jdbc:calcite:,driver,user,other"
  })
  void retainsExactUrlDriverAndCredentialIsolation(String url, String driver, String user, String password) {
    JdbcDataSourcePool owner = new JdbcDataSourcePool(BasicDataSource::new);
    try (JdbcDataSourcePool.Lease first = owner.acquire(URL, "driver", "user", "password");
        JdbcDataSourcePool.Lease same = owner.acquire(URL, "driver", "user", "password");
        JdbcDataSourcePool.Lease distinct = owner.acquire(url, driver, user, password)) {
      assertSame(first.dataSource(), same.dataSource());
      assertNotSame(first.dataSource(), distinct.dataSource());
    }
  }

  @Test
  void preservesNullVersusEmptyCredentialKeys() {
    JdbcDataSourcePool owner = new JdbcDataSourcePool(BasicDataSource::new);
    try (JdbcDataSourcePool.Lease unset = owner.acquire(URL, null, null, null);
        JdbcDataSourcePool.Lease empty = owner.acquire(URL, "", "", "")) {
      assertNotSame(unset.dataSource(), empty.dataSource());
    }
  }

  @Test
  void concurrentAcquisitionsReuseThePinnedPool() throws Exception {
    AtomicInteger created = new AtomicInteger();
    JdbcDataSourcePool owner = new JdbcDataSourcePool(() -> {
      created.incrementAndGet();
      return new BasicDataSource();
    });
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try (JdbcDataSourcePool.Lease pinned = owner.acquire(URL, null, null, null)) {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> work = new ArrayList<>();
      for (int worker = 0; worker < 4; worker++) {
        work.add(executor.submit(() -> {
          assertTrue(start.await(10, TimeUnit.SECONDS));
          for (int iteration = 0; iteration < 40; iteration++) {
            try (JdbcDataSourcePool.Lease lease = owner.acquire(URL, null, null, null)) {
              assertSame(pinned.dataSource(), lease.dataSource());
            }
          }
          return null;
        }));
      }
      start.countDown();
      for (Future<?> task : work) {
        task.get(20, TimeUnit.SECONDS);
      }
      assertEquals(1, created.get());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /** Isolated test-only capacity eviction; production retains Calcite-like soft-reference behavior. */
  public static final class CappedProbe {
    public static void main(String[] args) throws Exception {
      AtomicInteger created = new AtomicInteger();
      CalciteDriver driver = new CalciteDriver() {
        @Override protected void configureConnection(CalciteConnection connection, String url, Properties properties)
            throws SQLException {
          connection.setAutoCommit(true);
        }
      };
      JdbcDataSourcePool owner = new JdbcDataSourcePool(() -> {
        created.incrementAndGet();
        BasicDataSource source = new BasicDataSource();
        source.setDriver(driver);
        return source;
      });
      JdbcPoolCacheCap cap = new JdbcPoolCacheCap(owner, 1);
      AtomicInteger firstClosed = new AtomicInteger();
      AtomicInteger secondClosed = new AtomicInteger();
      AtomicInteger replacementClosed = new AtomicInteger();
      try (JdbcDataSourcePool.Lease first = owner.acquire(URL + "pool=first", null, null, null)) {
        assertEquals(1, cap.enforce());
        BasicDataSource firstSource = first.dataSource();
        recordPhysicalClose(firstSource, firstClosed);
        try (JdbcDataSourcePool.Lease second = owner.acquire(URL + "pool=second", null, null, null)) {
          assertEquals(1, cap.enforce());
          recordPhysicalClose(second.dataSource(), secondClosed);
        }
        try (JdbcDataSourcePool.Lease replacement = owner.acquire(URL + "pool=first", null, null, null)) {
          assertEquals(1, cap.enforce());
          assertNotSame(firstSource, replacement.dataSource());
          recordPhysicalClose(replacement.dataSource(), replacementClosed);
          awaitClose(secondClosed);
          assertFalse(firstSource.isClosed());
          try (Connection connection = firstSource.getConnection()) {
            assertFalse(connection.isClosed());
          }
          first.close();
          awaitClose(firstClosed);
          assertFalse(replacement.dataSource().isClosed());
          try (JdbcDataSourcePool.Lease again = owner.acquire(URL + "pool=first", null, null, null)) {
            assertSame(replacement.dataSource(), again.dataSource());
            assertEquals(1, cap.enforce());
          }
        }
      }
      assertEquals(0, new JdbcPoolCacheCap(owner, 0).enforce());
      awaitClose(replacementClosed);
      assertEquals(3, created.get());
      assertEquals(1, firstClosed.get());
      assertEquals(1, secondClosed.get());
      assertEquals(1, replacementClosed.get());
      System.out.println("capacity=1; created=3; reclaimed=3; active-usable=true");
    }

    private static void recordPhysicalClose(BasicDataSource source, AtomicInteger closed) throws SQLException {
      try (Connection connection = source.getConnection()) {
        CalciteDriver.addConnectionResource(connection.unwrap(CalciteConnection.class), closed::incrementAndGet);
      }
    }

    private static void awaitClose(AtomicInteger closed) throws InterruptedException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
      do {
        System.gc();
        if (closed.get() != 0) {
          break;
        }
        TimeUnit.MILLISECONDS.sleep(50);
      } while (System.nanoTime() < deadline);
      assertEquals(1, closed.get(), "Evicted pool must close its physical connection");
    }

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
      JdbcDataSourcePool owner = new JdbcDataSourcePool(() -> {
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
      JdbcDataSourcePool.Lease active = owner.acquire(ACTIVE_URL, null, null, null);
      BasicDataSource activeSource = active.dataSource();
      CalciteConnection activePhysical;
      try (Connection connection = activeSource.getConnection()) {
        activePhysical = connection.unwrap(CalciteConnection.class);
      }
      List<JdbcDataSourcePool.Lease> released = new ArrayList<>();
      for (int cycle = 0; cycle < 3; cycle++) {
        released.add(warmAndRelease(owner));
        int expectedCloses = closed.get() + 1;
        awaitReclamation(closed, expectedCloses);
        assertFalse(activeSource.isClosed());
        assertFalse(activePhysical.isClosed());
        try (JdbcDataSourcePool.Lease same = owner.acquire(ACTIVE_URL, null, null, null);
            Connection connection = same.dataSource().getConnection()) {
          assertSame(activeSource, same.dataSource());
          assertSame(activePhysical, connection.unwrap(CalciteConnection.class));
        }
      }
      warmAndAbandon(owner);
      int abandonedClose = closed.get() + 1;
      awaitReclamation(closed, abandonedClose);
      assertFalse(activeSource.isClosed());
      assertEquals(5, createdPools.get());
      assertEquals(failFirstClose ? 1 : 0, failures.get());
      int finalClose = closed.get() + 1;
      active.close();
      awaitReclamation(closed, finalClose);
      assertEquals(opened.get(), closed.get());
      Reference.reachabilityFence(released);
      Reference.reachabilityFence(active);
      System.out.println("reclaimed=5; created=" + createdPools.get()
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

    private static void warmAndAbandon(JdbcDataSourcePool owner) throws SQLException {
      JdbcDataSourcePool.Lease lease = owner.acquire(IDLE_URL, null, null, null);
      try (Connection connection = lease.dataSource().getConnection()) {
        assertTrue(connection.getAutoCommit());
      }
      Reference.reachabilityFence(lease);
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
      assertEquals(expected, closed.get(), "GC must dispose the idle physical connection");
    }
  }
}
