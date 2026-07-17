package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.DeploymentContext;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nullable;
import java.util.Properties;


/**
 * A {@link DeploymentContext} for the direct (non-SQL) API path. Unlike
 * {@link CalciteDeploymentContext}, it holds no Calcite {@link HoptimatorConnection}:
 *
 * <ul>
 *   <li>the table's row type is <em>carried</em> (the caller already produced it, e.g. from an
 *       Avro schema) rather than looked up from a Calcite {@code Table};
 *   <li>connection-level {@link #properties()} are a plain bag;
 *   <li>per-{@code Database} config is resolved through an injected {@link DatabaseConfigResolver},
 *       so this context does not depend on how the {@code Database} registry is stored.
 * </ul>
 *
 * <p>Because it is not {@link ConnectionBackedContext}, deployers cannot reach a
 * {@code java.sql.Connection} through it -- the direct path stays decoupled from Calcite.
 */
public final class DirectDeploymentContext implements DeploymentContext {

  private final Properties properties;
  private final DatabaseConfigResolver databaseConfigResolver;
  private final RelDataType rowType;

  public DirectDeploymentContext(Properties properties, DatabaseConfigResolver databaseConfigResolver,
      RelDataType rowType) {
    this.properties = properties;
    this.databaseConfigResolver = databaseConfigResolver;
    this.rowType = rowType;
  }

  /** The carried row type for the table being deployed. */
  public RelDataType rowType() {
    return rowType;
  }

  @Override
  public Properties properties() {
    return properties;
  }

  @Override
  public @Nullable Properties databaseProperties(@Nullable String catalog, String database,
      String connectionPrefix) {
    return databaseConfigResolver.databaseProperties(catalog, database, connectionPrefix);
  }
}
