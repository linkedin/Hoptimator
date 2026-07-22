package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.DatabaseConfigResolver;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import org.apache.calcite.avatica.ConnectStringParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * A {@link DatabaseConfigResolver} that reads {@code Database} config directly from K8s
 * {@code Database} CRDs — no Calcite catalog, no {@code java.sql.Connection}. It reconstructs the
 * same effective JDBC URL that {@link K8sDatabaseTable} would build for the catalog, then parses it
 * into connection properties, so the connection-free direct path resolves config identically to the
 * SQL path.
 */
public final class K8sDatabaseConfigResolver implements DatabaseConfigResolver {

  private static final Logger LOG = LoggerFactory.getLogger(K8sDatabaseConfigResolver.class);

  private final Properties connectionProperties;
  private final K8sContext context;
  private List<K8sDatabaseTable.Row> cachedDatabases;

  public K8sDatabaseConfigResolver(Properties connectionProperties) {
    this(connectionProperties, K8sContext.createFromProperties(connectionProperties));
  }

  /** Test seam: inject a (mockable) {@link K8sContext} instead of building one from the properties. */
  K8sDatabaseConfigResolver(Properties connectionProperties, K8sContext context) {
    this.connectionProperties = connectionProperties;
    this.context = context;
  }

  @Override
  public @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
      String connectionPrefix) {
    if (catalog == null && schema == null) {
      return null;
    }
    K8sDatabaseTable.Row row;
    try {
      row = findDatabase(catalog, schema);
    } catch (SQLException e) {
      // Fail loudly rather than returning null: the deployer providers treat a null here as "no
      // config for this store" and deploy nothing, which would report success while a K8s error
      // meant we never figured out what to deploy. The SPI signature can't throw, so wrap it.
      throw new IllegalStateException("Failed to resolve Database config for "
          + (catalog != null ? catalog : schema) + ": " + e.getMessage(), e);
    }
    if (row == null || row.URL == null || !row.URL.startsWith(connectionPrefix)) {
      return null;
    }
    String joined = K8sDatabaseTable.joinedUrl(row, connectionProperties);
    Properties properties = new Properties();
    try {
      properties.putAll(ConnectStringParser.parse(joined.substring(connectionPrefix.length())));
    } catch (SQLException e) {
      LOG.debug("Could not parse URL for schema '{}': {}", schema, e.getMessage());
      return null;
    }
    return properties;
  }

  @Override
  public String databaseName(List<String> tablePath) throws SQLException {
    String schema = tablePath.get(tablePath.size() - 2);
    String catalog = tablePath.size() >= 3 ? tablePath.get(tablePath.size() - 3) : null;
    K8sDatabaseTable.Row row = findDatabase(catalog, schema);
    if (row == null) {
      throw new SQLException("No Database is registered for "
          + (catalog != null ? catalog + "." + schema : schema) + ".");
    }
    // The database identifier is always the Database CRD name, for both schema- and catalog-style
    // Databases — matching the SQL path, which injects it into the JDBC URL as database=<name> (see
    // K8sDatabaseTable#joinedUrl). It names deployed resources and matches Table/Job template
    // `databases` filters; the store-level schema is carried separately by Source#schema().
    return row.NAME;
  }

  private @Nullable K8sDatabaseTable.Row findDatabase(@Nullable String catalog, @Nullable String schema)
      throws SQLException {
    for (K8sDatabaseTable.Row row : listDatabases()) {
      if (matches(row, catalog, schema)) {
        return row;
      }
    }
    return null;
  }

  /**
   * The Database CRDs, listed once and cached for this resolver's lifetime. A resolver is built once
   * per direct-path operation ({@code TableService.create}/{@code delete}), which may resolve
   * several databases (e.g. a logical table's tiers each hit this), so listing once per operation
   * avoids repeated K8s round-trips. Deliberately instance-scoped rather than static/global: a fresh
   * resolver per operation still observes newly created/deleted Databases.
   *
   * <p>TODO: This lists all Database CRDs and filters client-side ({@link #matches}) because the K8s
   * API cannot field-select on {@code spec.catalog}/{@code spec.schema} — only {@code metadata.name}
   * (via {@code K8sApi#get}) or labels (via {@code K8sApi#select(labelSelector)}). If Databases were
   * labelled with their catalog/schema at creation, the filter could be pushed server-side. Left as
   * list-and-filter for now: cardinality is low (one CRD per database) and it mirrors how
   * {@link K8sDatabaseTable} (the SQL/catalog path) enumerates Databases.
   */
  private List<K8sDatabaseTable.Row> listDatabases() throws SQLException {
    if (cachedDatabases == null) {
      List<K8sDatabaseTable.Row> rows = new ArrayList<>();
      // No catch: a failed list must surface (see databaseName/databaseProperties) rather than be
      // swallowed into an empty list that reads as "no Databases". cachedDatabases stays null on
      // failure, so it is not cached and a transient error can recover on the next call.
      for (V1alpha1Database db : new K8sApi<>(context, K8sApiEndpoints.DATABASES).list()) {
        rows.add(K8sDatabaseTable.rowOf(db));
      }
      cachedDatabases = rows;
    }
    return cachedDatabases;
  }

  private static boolean matches(K8sDatabaseTable.Row row, @Nullable String catalog, @Nullable String schema) {
    if (catalog != null) {
      // Catalog-style Database (e.g. MYSQL): config lives on the catalog CRD; the requested
      // `schema` is a sub-schema that shares this connection.
      return catalog.equalsIgnoreCase(row.CATALOG);
    }
    // Schema-style Database (e.g. KAFKA, VENICE): match by schema name.
    return row.CATALOG == null && schema != null && schema.equalsIgnoreCase(K8sDatabaseTable.schemaName(row));
  }
}
