package com.linkedin.hoptimator.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Properties;

import com.linkedin.hoptimator.DeploymentContext;


/**
 * A {@link DeploymentContext} backed by a Calcite {@link HoptimatorConnection}. This is the
 * context the SQL path supplies: {@link #databaseProperties} reads each {@code Database}'s JDBC
 * URL from the Calcite catalog, and the table's row type is resolved from the Calcite catalog
 * (see {@link HoptimatorDriver#rowType}).
 */
public final class CalciteDeploymentContext implements DeploymentContext {

  private static final Logger LOG = LoggerFactory.getLogger(CalciteDeploymentContext.class);

  private final HoptimatorConnection connection;

  public CalciteDeploymentContext(HoptimatorConnection connection) {
    this.connection = connection;
  }

  /** The underlying connection. Retained for the Calcite-only planning path (not the deploy SPI). */
  public HoptimatorConnection connection() {
    return connection;
  }

  @Override
  public Properties properties() {
    return connection.connectionProperties();
  }

  @Override
  public @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
      String connectionPrefix) throws SQLException {
    return DeployerUtils.extractPropertiesFromJdbcSchema(catalog, schema, connection, connectionPrefix);
  }
}
