package com.linkedin.hoptimator.venice;

import com.linkedin.hoptimator.jdbc.CalciteDriver;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientConnectionException;
import java.util.Properties;


/** JDBC driver for Venice stores. */
public class VeniceDriver extends CalciteDriver {
  public static final String CATALOG_NAME = "VENICE";
  public static final String CONNECTION_PREFIX = "jdbc:venice://";

  static {
    new VeniceDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECTION_PREFIX;
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(this.getClass(), "venice.properties", "venice", "0", "venice", "0");
  }

  @Override
  protected void configureConnection(CalciteConnection connection, String url, Properties properties) throws SQLException {
    connection.setAutoCommit(true); // to prevent rollback()
    connection.setCatalog(CATALOG_NAME);
    SchemaPlus rootSchema = connection.getRootSchema();
    ClusterSchema schema = createClusterSchema(properties);
    if (schema instanceof AutoCloseable) {
      addConnectionResource(connection, (AutoCloseable) schema);
    }
    rootSchema.add(CATALOG_NAME, schema);
  }

  @Override
  protected SQLException connectionException(String url, Exception failure) {
    if (failure instanceof IOException) {
      return new SQLTransientConnectionException("Problem loading " + url, failure);
    }
    return new SQLNonTransientException("Problem loading " + url, failure);
  }

  protected ClusterSchema createClusterSchema(Properties properties) {
    return new ClusterSchema(properties);
  }
}
