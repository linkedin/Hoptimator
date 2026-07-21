package com.linkedin.hoptimator.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;


/**
 * Resolves per-{@code Database} connection config, decoupled from how the {@code Database} registry
 * is stored. This is the seam that lets a {@link com.linkedin.hoptimator.DeploymentContext} answer
 * {@code databaseProperties(...)} without holding a Calcite {@link HoptimatorConnection}: the SQL
 * path supplies a resolver that reads the Calcite catalog; the direct path can supply a resolver
 * that reads {@code Database} CRDs (or any other registry) directly.
 */
public interface DatabaseConfigResolver {

  /**
   * Returns the parsed connection properties for a database, or {@code null} when the database is
   * unknown or its URL does not start with {@code connectionPrefix}. The {@code Database} is keyed
   * by a catalog and/or a schema; both are individually optional, but at least one must be provided.
   *
   * @param catalog          the catalog name, or {@code null}
   * @param schema           the schema name, or {@code null}
   * @param connectionPrefix the expected URL scheme prefix (e.g. {@code "jdbc:kafka://"})
   */
  @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
      String connectionPrefix);

  /**
   * Resolves the {@code database} identifier for a table at {@code tablePath} — the value exposed as
   * {@link com.linkedin.hoptimator.Source#database()} and used to name/derive deployed resources.
   * For a schema-style database this is the {@code Database} name; for a catalog-style database it
   * is the schema segment of the path (an independent sub-database sharing the catalog connection).
   */
  String databaseName(List<String> tablePath) throws SQLException;
}
