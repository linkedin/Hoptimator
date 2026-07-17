package com.linkedin.hoptimator.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Properties;


/**
 * A {@link DeploymentContext} backed by a Calcite {@link HoptimatorConnection}. This is the
 * context the SQL path supplies: {@link #databaseProperties} reads each {@code Database}'s JDBC
 * URL from the Calcite catalog, and the table's row type is resolved from the Calcite catalog
 * (see {@link HoptimatorDriver#rowType}).
 */
public final class CalciteDeploymentContext implements ConnectionBackedContext {

  private static final Logger LOG = LoggerFactory.getLogger(CalciteDeploymentContext.class);

  private final HoptimatorConnection connection;

  public CalciteDeploymentContext(HoptimatorConnection connection) {
    this.connection = connection;
  }

  /** The underlying connection. Retained for the Calcite-only planning path (not the deploy SPI). */
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
