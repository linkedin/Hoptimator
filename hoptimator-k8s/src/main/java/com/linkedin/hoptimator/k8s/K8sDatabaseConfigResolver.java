package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.DatabaseConfigResolver;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import org.apache.calcite.avatica.ConnectStringParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.SQLException;
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
  private K8sContext context;

  public K8sDatabaseConfigResolver(Properties connectionProperties) {
    this.connectionProperties = connectionProperties;
  }

  @Override
  public @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
      String connectionPrefix) {
    if (catalog == null && schema == null) {
      return null;
    }
    K8sDatabaseTable.Row row = findDatabase(catalog, schema);
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

  private @Nullable K8sDatabaseTable.Row findDatabase(@Nullable String catalog, @Nullable String schema) {
    try {
      for (V1alpha1Database db : api().list()) {
        K8sDatabaseTable.Row row = K8sDatabaseTable.rowOf(db);
        if (matches(row, catalog, schema)) {
          return row;
        }
      }
    } catch (SQLException e) {
      LOG.debug("Could not list Database CRDs while resolving config for '{}': {}", schema, e.getMessage());
    }
    return null;
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

  private K8sApi<V1alpha1Database, V1alpha1DatabaseList> api() {
    if (context == null) {
      context = K8sContext.createFromProperties(connectionProperties);
    }
    return new K8sApi<>(context, K8sApiEndpoints.DATABASES);
  }
}
