package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.DeploymentContext;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Properties;


/**
 * A {@link DeploymentContext} for the direct (non-SQL) API path. Unlike
 * {@link CalciteDeploymentContext}, it holds no Calcite {@link HoptimatorConnection}:
 *
 * <ul>
 *   <li>the table's row type is <em>carried</em> (the caller already produced it, e.g. from an
 *       Avro schema) rather than looked up from a Calcite {@code Table};
 *   <li>the caller's original Avro schema is carried alongside the row type, so deployers that
 *       speak Avro (schema registry, Venice, ...) can use it verbatim instead of re-synthesizing
 *       from the row type — the Avro&nbsp;&rarr;&nbsp;RelDataType&nbsp;&rarr;&nbsp;Avro round-trip is
 *       lossy (namespaces, nested record names, unions, defaults);
 *   <li>connection-level {@link #properties()} are a plain bag;
 *   <li>per-{@code Database} config is resolved through an injected {@link DatabaseConfigResolver},
 *       so this context does not depend on how the {@code Database} registry is stored.
 * </ul>
 *
 * <p>Because it is not a {@link CalciteDeploymentContext}, deployers cannot reach a
 * {@code java.sql.Connection} through it -- the direct path stays decoupled from Calcite.
 */
public final class DirectDeploymentContext implements DeploymentContext {

  private final Properties properties;
  private final DatabaseConfigResolver databaseConfigResolver;
  private final @Nullable RelDataType rowType;
  private final @Nullable Schema avroSchema;

  public DirectDeploymentContext(Properties properties, DatabaseConfigResolver databaseConfigResolver,
      @Nullable RelDataType rowType) {
    this(properties, databaseConfigResolver, rowType, null);
  }

  public DirectDeploymentContext(Properties properties, DatabaseConfigResolver databaseConfigResolver,
      @Nullable RelDataType rowType, @Nullable Schema avroSchema) {
    this.properties = properties;
    this.databaseConfigResolver = databaseConfigResolver;
    this.rowType = rowType;
    this.avroSchema = avroSchema;
  }

  /** The carried row type for the table being deployed, or {@code null} for schema-free operations
   * such as delete. */
  public RelDataType rowType() {
    if (rowType == null) {
      throw new IllegalStateException("No row type is carried by this context (e.g. a delete). "
          + "A deployer requested a row type on a schema-free operation.");
    }
    return rowType;
  }

  /**
   * The caller's original (merged key+value) Avro schema for the table being deployed, or
   * {@code null} when the caller supplied none (e.g. a delete, or a caller that only provided a
   * row type). Deployers should prefer this over re-synthesizing Avro from {@link #rowType()},
   * since the row type cannot represent Avro namespaces, nested record identities, unions, or
   * defaults.
   */
  public @Nullable Schema avroSchema() {
    return avroSchema;
  }

  @Override
  public Properties properties() {
    return properties;
  }

  @Override
  public @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
      String connectionPrefix) throws SQLException {
    return databaseConfigResolver.databaseProperties(catalog, schema, connectionPrefix);
  }
}
