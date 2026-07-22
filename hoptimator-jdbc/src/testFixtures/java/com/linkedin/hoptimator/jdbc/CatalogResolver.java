package com.linkedin.hoptimator.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;


/**
 * Resolves a table's schema from the live {@code k8s} catalog — the programmatic equivalent of
 * quidem's {@code !describe}. Used by the {@code *TableServiceIntegrationTest}s to verify that a
 * table created via the SQL-free {@link TableService} really registered (and, after delete, really
 * went away), independently of what {@code create} returned.
 *
 * <p>Each call opens a fresh connection so the catalog is re-read (Calcite caches per connection),
 * and navigation follows the table path: {@code [SCHEMA, TABLE]} or {@code [CATALOG, SCHEMA, TABLE]}.
 * Note: this reliably reflects <em>creates</em>, but not <em>deletes</em> — Calcite's
 * {@code JdbcSchema}/{@code ClusterSchema} caches table existence at a level shared across
 * connections, so a dropped table can still resolve. Use it to verify registration, not removal.
 */
public final class CatalogResolver {

  private static final long DEFAULT_TIMEOUT_MILLIS = 60_000;
  private static final long POLL_INTERVAL_MILLIS = 2_000;

  private CatalogResolver() {
  }

  /** Resolves the row type for {@code path}, or {@code null} if the table is not present. */
  public static RelDataType resolveRowType(List<String> path) {
    try (HoptimatorConnection conn =
        (HoptimatorConnection) DriverManager.getConnection("jdbc:hoptimator://catalogs=k8s")) {
      SchemaPlus schema = conn.calciteConnection().getRootSchema();
      for (String segment : path.subList(0, path.size() - 1)) {
        if (schema == null) {
          return null;
        }
        schema = schema.subSchemas().get(segment);
      }
      if (schema == null) {
        return null;
      }
      Table table = schema.tables().get(path.get(path.size() - 1));
      return table == null ? null : table.getRowType(conn.calciteConnection().getTypeFactory());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Polls until {@code path} resolves (store provisioning may be asynchronous), then returns it. */
  public static RelDataType awaitResolved(List<String> path) throws SQLException {
    long deadline = System.currentTimeMillis() + DEFAULT_TIMEOUT_MILLIS;
    RuntimeException last = null;
    while (System.currentTimeMillis() < deadline) {
      try {
        RelDataType rowType = resolveRowType(path);
        if (rowType != null) {
          return rowType;
        }
      } catch (RuntimeException e) {
        last = e;
      }
      sleep();
    }
    throw last != null ? new SQLException(last)
        : new SQLException("Table not resolvable after create: " + path);
  }

  /** Polls until {@code path} no longer resolves (teardown may be asynchronous). */
  public static void awaitAbsent(List<String> path) throws SQLException {
    long deadline = System.currentTimeMillis() + DEFAULT_TIMEOUT_MILLIS;
    while (System.currentTimeMillis() < deadline) {
      try {
        if (resolveRowType(path) == null) {
          return;
        }
      } catch (RuntimeException ignored) {
        // Mid-teardown loads can fail transiently; keep polling until a clean absent.
      }
      sleep();
    }
    throw new SQLException("Table still resolvable after delete: " + path);
  }

  private static void sleep() throws SQLException {
    try {
      Thread.sleep(POLL_INTERVAL_MILLIS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException(e);
    }
  }
}
