package com.linkedin.hoptimator;

import java.util.Properties;

import javax.annotation.Nullable;


/**
 * The ambient information a {@link Deployer}, {@link Connector}, or {@link Validator} needs in
 * order to act on a {@link Deployable}, independent of how it was produced.
 *
 * <p>This replaces passing a raw {@code java.sql.Connection} through the SPI. The SQL path
 * supplies a Calcite-backed implementation; a direct (non-SQL) caller supplies an
 * implementation backed by its own control plane. Neither the deploy nor validate machinery
 * needs to know which producer it came from.
 *
 * <p>A context exposes only two things:
 * <ul>
 *   <li>connection-level {@link #properties()} (namespace, hints, cluster config);
 *   <li>per-{@code Database} connection config via {@link #databaseProperties}.
 * </ul>
 *
 * <p>A table's row type is <em>not</em> exposed here (that would couple this API to a schema
 * representation). It is resolved by the producer-specific machinery: the SQL path reads it from
 * the Calcite catalog; the direct path carries it on its own context implementation.
 */
public interface DeploymentContext {

  /** Connection-level properties and hints (e.g. namespace, {@code k8s.*}, hints, mode). */
  Properties properties();

  /**
   * Returns the parsed connection properties for a database, extracted from its connection URL
   * after stripping {@code connectionPrefix}, or {@code null} when the database is unknown or its
   * URL does not start with {@code connectionPrefix}.
   *
   * @param catalog          optional catalog name, or {@code null}
   * @param database         the database (schema) name
   * @param connectionPrefix the expected URL scheme prefix (e.g. {@code "jdbc:kafka://"})
   */
  @Nullable Properties databaseProperties(@Nullable String catalog, String database, String connectionPrefix);
}
