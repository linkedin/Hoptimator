package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.DeploymentContext;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Properties;


/**
 * A {@link DeploymentContext} for the direct (non-SQL) API path. Unlike
 * {@link CalciteDeploymentContext}, it does <em>not</em> resolve the table's row type by looking up
 * a Calcite {@code Table} — instead it <em>carries</em> the resolved {@link RelDataType} that the
 * caller already produced (e.g. from an Avro schema). This is what lets the direct path skip
 * registering a temporary Calcite table just to hand deployers a schema.
 *
 * <p>The underlying {@link HoptimatorConnection} is still used for {@code Database}-registry config
 * ({@link #databaseProperties}) — that is catalog metadata, independent of any single table's
 * schema. Resolving Database config without Calcite is future work.
 */
public final class DirectDeploymentContext implements ConnectionBackedContext {

  private static final Logger LOG = LoggerFactory.getLogger(DirectDeploymentContext.class);

  private final HoptimatorConnection connection;
  private final RelDataType rowType;

  public DirectDeploymentContext(HoptimatorConnection connection, RelDataType rowType) {
    this.connection = connection;
    this.rowType = rowType;
  }

  /** The carried row type for the table being deployed. */
  public RelDataType rowType() {
    return rowType;
  }

  @Override
  public HoptimatorConnection connection() {
    return connection;
  }

  @Override
  public Properties properties() {
    return connection.connectionProperties();
  }

  @Override
  public @Nullable Properties databaseProperties(@Nullable String catalog, String database,
      String connectionPrefix) {
    return DeployerUtils.extractPropertiesFromJdbcSchema(catalog, database, connection, connectionPrefix, LOG);
  }
}
