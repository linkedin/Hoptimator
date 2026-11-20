package com.linkedin.hoptimator.jdbc;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;


/** Reuses softly retained JDBC pools and closes them when their unleased owners are reclaimed. */
public final class JdbcDataSourcePool implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcDataSourcePool.class);
  private static final Cleaner CLEANER = Cleaner.create();

  private final Map<List<String>, Entry> pools = new HashMap<>();
  private final Set<Entry> owned = new HashSet<>();
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

  /** Pins a pool for the entire catalog lifetime; keep the lease open while using its data source. */
  public synchronized Lease acquire(String url, @Nullable String driver,
      @Nullable String user, @Nullable String password) throws SQLException {
    if (closed) {
      throw new SQLException("JDBC pool owner is closed");
    }
    List<String> key = key(url, driver, user, password);
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
      owned.add(entry);
      pools.put(key, entry);
    }
    Entry acquired = requireNonNull(entry);
    acquired.leases++;
    return new Lease(acquired, token);
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
      for (Entry entry : new ArrayList<>(owned)) {
        retire(entry, disposable);
      }
      pools.clear();
    }
    dispose(disposable);
  }

  private static List<String> key(String url, @Nullable String driver,
      @Nullable String user, @Nullable String password) {
    // Match Calcite's existing key exactly; do not normalize hints, credentials, or nulls.
    return Arrays.asList(requireNonNull(url, "url"), user, password, driver);
  }

  private void retire(Entry entry, List<Entry> disposable) {
    entry.retired = true;
    if (entry.leases == 0 || entry.get() == null) {
      claim(entry, disposable);
    }
  }

  private void claim(Entry entry, List<Entry> disposable) {
    if (!entry.disposed) {
      entry.disposed = true;
      owned.remove(entry);
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
      } finally {
        entry.cleanable.clean();
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  // Cleanup owns the pool, never its softly referenced token. Active catalog leases pin the token.
  private final class Entry extends SoftReference<Object> implements Runnable {
    private final List<String> key;
    private final BasicDataSource source;
    private final Cleaner.Cleanable cleanable;
    private int leases;
    private boolean retired;
    private boolean disposed;

    private Entry(Object token, List<String> key, BasicDataSource source) {
      super(token);
      this.key = key;
      this.source = source;
      cleanable = CLEANER.register(token, this);
    }

    @Override public void run() {
      List<Entry> disposable = new ArrayList<>();
      synchronized (JdbcDataSourcePool.this) {
        if (disposed) {
          return;
        }
        pools.remove(key, this);
        // No reachable lease can remain once its token is phantom reachable, even if not closed.
        claim(this, disposable);
      }
      try {
        dispose(disposable);
      } catch (SQLException e) {
        LOG.error("Failed to close reclaimed JDBC pool", e);
      }
    }
  }

  public final class Lease implements AutoCloseable {
    private final Entry entry;
    @Nullable private Object token;
    private boolean released;

    private Lease(Entry entry, Object token) {
      this.entry = entry;
      this.token = token;
    }

    public BasicDataSource dataSource() {
      synchronized (JdbcDataSourcePool.this) {
        if (released) {
          throw new IllegalStateException("JDBC catalog pool lease is closed");
        }
        return entry.source;
      }
    }

    /** Releases catalog ownership; disposal failures are logged, not failures of the catalog operation. */
    @Override
    public void close() throws SQLException {
      List<Entry> disposable = new ArrayList<>();
      Object pinned;
      synchronized (JdbcDataSourcePool.this) {
        if (released) {
          return;
        }
        released = true;
        pinned = token;
        token = null;
        entry.leases--;
        if (entry.retired && entry.leases == 0) {
          claim(entry, disposable);
        }
      }
      try {
        dispose(disposable);
      } catch (SQLException e) {
        LOG.warn("Failed to close retired JDBC pool after catalog lease release", e);
      } finally {
        Reference.reachabilityFence(pinned);
      }
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
