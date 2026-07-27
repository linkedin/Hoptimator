package com.linkedin.hoptimator;

import java.sql.SQLException;
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
   * <p>The {@code Database} it identifies is keyed by a catalog and/or a schema: catalog-style
   * databases (e.g. MySQL) are matched by catalog, while schema-style databases (e.g. Kafka,
   * Venice) are matched by schema. Both are individually optional — mirroring the {@code Database}
   * CRD, where {@code catalog} and {@code schema} are optional — but at least one must be provided.
   *
   * @param catalog          the catalog name, or {@code null}
   * @param schema           the schema name, or {@code null}
   * @param connectionPrefix the expected URL scheme prefix (e.g. {@code "jdbc:kafka://"})
   */
  @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
      String connectionPrefix) throws SQLException;
}
