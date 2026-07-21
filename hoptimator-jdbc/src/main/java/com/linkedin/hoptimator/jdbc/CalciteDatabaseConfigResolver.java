package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.List;
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
  public @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
      String connectionPrefix) {
    return DeployerUtils.extractPropertiesFromJdbcSchema(catalog, schema, connection, connectionPrefix, LOG);
  }

  @Override
  public String databaseName(List<String> tablePath) throws SQLException {
    CalcitePrepare.Context ctx = connection.createPrepareContext();
    SqlIdentifier name = new SqlIdentifier(tablePath, SqlParserPos.ZERO);
    return HoptimatorDdlUtils.resolveCreateTarget(ctx, connection, false, name).database;
  }
}
