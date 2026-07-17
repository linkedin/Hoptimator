package com.linkedin.hoptimator;

import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.avro.Schema;


/**
 * The ambient information a {@link Deployer}, {@link Connector}, or {@link Validator} needs in
 * order to act on a {@link Deployable}, independent of how it was produced.
 *
 * <p>This replaces passing a raw {@code java.sql.Connection} through the SPI. The SQL path
 * supplies a Calcite-backed implementation; a direct (non-SQL) caller supplies an
 * implementation backed by its own control plane. Neither the deploy nor validate machinery
 * needs to know which producer it came from.
 *
 * <p>A context exposes only three things:
 * <ul>
 *   <li>connection-level {@link #properties()} (namespace, hints, cluster config);
 *   <li>per-{@code Database} connection config via {@link #databaseProperties};
 *   <li>the currently-resolved schema for a table via {@link #existingSchema}.
 * </ul>
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

  /**
   * Returns the currently-resolved value Avro schema for the table at {@code path} when it is
   * backed by native Avro metadata, or {@code null} otherwise. Consumers fall back to the
   * deployable's own {@link Source#rowSchema()} when this returns {@code null}.
   */
  @Nullable Schema existingSchema(List<String> path);
}
