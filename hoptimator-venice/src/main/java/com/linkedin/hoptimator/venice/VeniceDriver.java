package com.linkedin.hoptimator.venice;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.SchemaPlus;


/** JDBC driver for Venice stores. */
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
    // Connection string properties are given precedence over config properties
    Properties properties = new Properties();
    properties.putAll(props); // in case the driver is loaded via getConnection()
    properties.putAll(ConnectStringParser.parse(url.substring(getConnectStringPrefix().length())));

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
      rootSchema.add(CATALOG_NAME, schema);
      return connection;
    } catch (IOException|ExecutionException|InterruptedException e) {
      throw new SQLTransientConnectionException("Problem loading " + url, e);
    } catch (Exception e) {
      throw new SQLNonTransientException("Problem loading " + url, e);
    }
  }

  protected ClusterSchema createClusterSchema(Properties properties) {
    return new ClusterSchema(properties);
  }
}
