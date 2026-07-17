package com.linkedin.hoptimator.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Properties;


/**
 * A {@link DatabaseConfigResolver} backed by the Calcite catalog of a {@link HoptimatorConnection}:
 * it reads each {@code Database}'s JDBC URL from the JDBC schema registered under the catalog. This
 * is the resolver the SQL path uses, and the default the direct path falls back to when no
 * registry-native resolver is available.
 */
public final class CalciteDatabaseConfigResolver implements DatabaseConfigResolver {

  private static final Logger LOG = LoggerFactory.getLogger(CalciteDatabaseConfigResolver.class);

  private final HoptimatorConnection connection;

  public CalciteDatabaseConfigResolver(HoptimatorConnection connection) {
    this.connection = connection;
  }

  @Override
  public @Nullable Properties databaseProperties(@Nullable String catalog, String database,
      String connectionPrefix) {
    return DeployerUtils.extractPropertiesFromJdbcSchema(catalog, database, connection, connectionPrefix, LOG);
  }
}
