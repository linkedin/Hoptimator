package com.linkedin.hoptimator.venice;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.SchemaPlus;


/** JDBC driver for Venice topics. */
public class VeniceDriver extends Driver {

  public static final String CATALOG_NAME = "VENICE";

  static {
    new VeniceDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return "jdbc:venice://";
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(this.getClass(), "venice.properties", "venice", "0", "venice", "0");
  }

  @Override
  public Connection connect(String url, Properties props) throws SQLException {
    if (!url.startsWith(getConnectStringPrefix())) {
      return null;
    }
    Properties properties = ConnectStringParser.parse(url.substring(getConnectStringPrefix().length()));
    String cluster = properties.getProperty("cluster");
    if (cluster == null) {
      throw new IllegalArgumentException("Missing required cluster property. Need: jdbc:venice://cluster=...");
    }
    cluster = cluster.toUpperCase(Locale.ROOT);
    if (!cluster.startsWith(CATALOG_NAME)) {
      cluster = CATALOG_NAME + "-" + cluster;
    }
    try {
      Connection connection = super.connect(url, props);
      if (connection == null) {
        throw new IOException("Could not connect to " + url);
      }
      connection.setAutoCommit(true); // to prevent rollback()
      connection.setCatalog(CATALOG_NAME);
      CalciteConnection calciteConnection = (CalciteConnection) connection;
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      ClusterSchema schema = createClusterSchema(properties);
      schema.populate();
      rootSchema.add(cluster.toUpperCase(Locale.ROOT), schema);
      return connection;
    } catch (Exception e) {
      throw new SQLException("Problem loading " + url, e);
    }
  }

  protected ClusterSchema createClusterSchema(Properties properties) {
    return new ClusterSchema(properties);
  }
}
