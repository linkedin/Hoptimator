package com.linkedin.hoptimator.jdbc;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.ref.Cleaner;
import java.lang.ref.SoftReference;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;


/**
 * Caches shared DBCP data sources by their exact JDBC connection settings, like Calcite's soft cache.
 * A catalog handle pins its pool while the catalog can use it; closing the handle does not close the
 * pool, which remains available for reuse. Once no handles pin a pool, memory pressure may clear its
 * soft reference. A Cleaner then closes the data source and its idle physical connections, whose
 * cleanup is not guaranteed by garbage collection alone. There is no time-based eviction or
 * deterministic shutdown cleanup; checked-out connections are closed when returned to a closed pool.
 */
public final class JdbcDataSourcePool {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcDataSourcePool.class);
  private static final Cleaner CLEANER = Cleaner.create();

  private final Map<List<String>, Entry> pools = new HashMap<>();
  private final Supplier<BasicDataSource> factory;

  private JdbcDataSourcePool() {
    this(BasicDataSource::new);
  }

  JdbcDataSourcePool(Supplier<BasicDataSource> factory) {
    this.factory = requireNonNull(factory, "factory");
  }

  public static JdbcDataSourcePool shared() {
    return Shared.INSTANCE;
  }

  /** Pins a pool for the entire catalog lifetime; keep the handle open while using its data source. */
  public synchronized Lease acquire(String url, @Nullable String driver,
      @Nullable String user, @Nullable String password) {
    List<String> key = Arrays.asList(requireNonNull(url, "url"), user, password, driver);
    Entry entry = pools.get(key);
    Object token = entry == null ? null : entry.get();
    if (token == null) {
      BasicDataSource source = factory.get();
      source.setUrl(url);
      source.setDriverClassName(driver);
      source.setUsername(user);
      source.setPassword(password);
      token = new Object();
      entry = new Entry(token, key, source);
      pools.put(key, entry);
    }
    return new Lease(requireNonNull(entry), token);
  }

  // The cleanup action owns the pool but must not strongly reference its token.
  private final class Entry extends SoftReference<Object> implements Runnable {
    private final List<String> key;
    private final BasicDataSource source;

    private Entry(Object token, List<String> key, BasicDataSource source) {
      super(token);
      this.key = key;
      this.source = source;
      CLEANER.register(token, this);
    }

    @Override public void run() {
      synchronized (JdbcDataSourcePool.this) {
        pools.remove(key, this);
      }
      try {
        LOG.info("Closing reclaimed JDBC pool driver={}", source.getDriverClassName());
        source.close();
        LOG.info("Closed reclaimed JDBC pool driver={}", source.getDriverClassName());
      } catch (SQLException | RuntimeException e) {
        LOG.error("Failed to close reclaimed JDBC pool", e);
      }
    }
  }

  public static final class Lease implements AutoCloseable {
    private final Entry entry;
    @Nullable private Object token;

    private Lease(Entry entry, Object token) {
      this.entry = entry;
      this.token = token;
    }

    public synchronized BasicDataSource dataSource() {
      if (token == null) {
        throw new IllegalStateException("JDBC catalog pool lease is closed");
      }
      return entry.source;
    }

    /** Releases the catalog's pin without closing the reusable pool. */
    @Override
    public synchronized void close() {
      token = null;
    }
  }

  private static final class Shared {
    private static final JdbcDataSourcePool INSTANCE = new JdbcDataSourcePool();
  }
}
