package com.linkedin.hoptimator.jdbc;

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
   * unknown or its URL does not start with {@code connectionPrefix}.
   *
   * @param catalog          optional catalog name, or {@code null}
   * @param database         the database (schema) name
   * @param connectionPrefix the expected URL scheme prefix (e.g. {@code "jdbc:kafka://"})
   */
  @Nullable Properties databaseProperties(@Nullable String catalog, String database, String connectionPrefix);
}
