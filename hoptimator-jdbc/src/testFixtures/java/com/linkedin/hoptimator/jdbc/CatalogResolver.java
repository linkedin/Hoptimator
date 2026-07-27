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

  private CatalogResolver() {
  }

  /** Resolves the row type for {@code path}, or {@code null} if the table is not present. */
  private static RelDataType resolveRowType(List<String> path) {
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

  /**
   * Resolves {@code path} immediately, failing if it is not present. The store deployers run
   * synchronously, so a table must be resolvable right after {@code create} returns
   */
  public static RelDataType resolve(List<String> path) throws SQLException {
    RelDataType rowType = resolveRowType(path);
    if (rowType == null) {
      throw new SQLException("Table not resolvable after create: " + path);
    }
    return rowType;
  }
}
