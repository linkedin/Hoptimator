package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.avro.AvroSchemaSource;
import org.apache.avro.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;


/**
 * A {@link DeploymentContext} backed by a Calcite {@link HoptimatorConnection}. This is the
 * context the SQL path supplies: {@link #databaseProperties} reads each {@code Database}'s JDBC
 * URL from the Calcite catalog, and {@link #existingSchema} reads native Avro metadata from the
 * resolved Calcite table.
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
  public @Nullable Properties databaseProperties(@Nullable String catalog, String database,
      String connectionPrefix) {
    return DeployerUtils.extractPropertiesFromJdbcSchema(catalog, database, connection, connectionPrefix, LOG);
  }

  @Override
  public @Nullable Schema existingSchema(List<String> path) {
    SchemaPlus schema = connection.calciteConnection().getRootSchema();
    for (String part : Util.skipLast(path)) {
      if (schema == null) {
        return null;
      }
      schema = schema.subSchemas().get(part);
    }
    if (schema == null) {
      return null;
    }
    Table table = schema.tables().get(path.get(path.size() - 1));
    return table instanceof AvroSchemaSource ? ((AvroSchemaSource) table).valueSchema() : null;
  }
}
