package com.linkedin.hoptimator.jdbc;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;


/** Owns shared JDBC pools independently of cache reachability and individual JDBC leases. */
public final class JdbcDataSourcePool implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcDataSourcePool.class);

  private final Map<List<String>, Entry> pools = new HashMap<>();
  private final Supplier<BasicDataSource> factory;
  private boolean closed;

  public JdbcDataSourcePool() {
    this(BasicDataSource::new);
  }

  JdbcDataSourcePool(Supplier<BasicDataSource> factory) {
    this.factory = requireNonNull(factory, "factory");
  }

  public static JdbcDataSourcePool shared() {
    return Shared.INSTANCE;
  }

  /** Holds a pool for the entire catalog lifetime, including lazy future metadata reads. */
  public synchronized Lease acquire(String url, @Nullable String driver,
      @Nullable String user, @Nullable String password) throws SQLException {
    if (closed) {
      throw new SQLException("JDBC pool owner is closed");
    }
    List<String> key = key(url, driver, user, password);
    Entry entry = pools.get(key);
    if (entry == null) {
      BasicDataSource source = factory.get();
      source.setUrl(url);
      source.setDriverClassName(driver);
      source.setUsername(user);
      source.setPassword(password);
      entry = new Entry(source);
      pools.put(key, entry);
    }
    entry.leases++;
    return new Lease(entry);
  }

  /** Retires one exact pool key without disrupting catalogs already holding it. */
  public void invalidate(String url, @Nullable String driver,
      @Nullable String user, @Nullable String password) throws SQLException {
    List<Entry> disposable = new ArrayList<>();
    synchronized (this) {
      Entry entry = pools.remove(key(url, driver, user, password));
      if (entry != null) {
        retire(entry, disposable);
      }
    }
    dispose(disposable);
  }

  /** Retires cached pools; subsequent acquisitions create new pools. */
  public void invalidateAll() throws SQLException {
    retireAll(false);
  }

  /** Rejects new acquisitions; leased pools close when their last catalog releases them. */
  @Override
  public void close() throws SQLException {
    retireAll(true);
  }

  private void retireAll(boolean terminal) throws SQLException {
    List<Entry> disposable = new ArrayList<>();
    synchronized (this) {
      closed |= terminal;
      pools.values().forEach(entry -> retire(entry, disposable));
      pools.clear();
    }
    dispose(disposable);
  }

  private static List<String> key(String url, @Nullable String driver,
      @Nullable String user, @Nullable String password) {
    // Match Calcite's existing key exactly; do not normalize hints, credentials, or nulls.
    return Arrays.asList(requireNonNull(url, "url"), user, password, driver);
  }

  private static void retire(Entry entry, List<Entry> disposable) {
    entry.retired = true;
    if (entry.leases == 0) {
      disposable.add(entry);
    }
  }

  private static void dispose(List<Entry> entries) throws SQLException {
    SQLException failure = null;
    for (Entry entry : entries) {
      try {
        entry.source.close();
      } catch (SQLException | RuntimeException e) {
        if (failure == null) {
          failure = new SQLException("Failed to close retired JDBC pools", e);
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static final class Entry {
    private final BasicDataSource source;
    private int leases;
    private boolean retired;

    private Entry(BasicDataSource source) {
      this.source = source;
    }
  }

  public final class Lease implements AutoCloseable {
    private final Entry entry;
    private boolean released;

    private Lease(Entry entry) {
      this.entry = entry;
    }

    public BasicDataSource dataSource() {
      synchronized (JdbcDataSourcePool.this) {
        if (released) {
          throw new IllegalStateException("JDBC catalog pool lease is closed");
        }
        return entry.source;
      }
    }

    @Override
    public void close() throws SQLException {
      List<Entry> disposable = new ArrayList<>();
      synchronized (JdbcDataSourcePool.this) {
        if (released) {
          return;
        }
        released = true;
        entry.leases--;
        if (entry.retired && entry.leases == 0) {
          disposable.add(entry);
        }
      }
      dispose(disposable);
    }
  }

  private static final class Shared {
    private static final JdbcDataSourcePool INSTANCE = create();

    private static JdbcDataSourcePool create() {
      JdbcDataSourcePool owner = new JdbcDataSourcePool();
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          owner.close();
        } catch (SQLException e) {
          LOG.error("Failed to shut down shared JDBC pools", e);
        }
      }, "hoptimator-jdbc-pools-shutdown"));
      return owner;
    }
  }
}
