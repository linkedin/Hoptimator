package com.linkedin.hoptimator.pinot;

import com.linkedin.hoptimator.jdbc.CalciteDriver;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientConnectionException;
import java.util.Properties;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

/** JDBC driver for Pinot tables. */
public class PinotDriver extends CalciteDriver {

  public static final String CATALOG_NAME = "PINOT";
  public static final String CONNECTION_PREFIX = "jdbc:pinot://";
  public static final String CONTROLLER_URL = "controllerUrl";

  static {
    new PinotDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECTION_PREFIX;
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(this.getClass(), "pinot.properties", "pinot", "0", "pinot", "0");
  }

  @Override
  public Connection connect(String url, Properties props) throws SQLException {
    if (!url.startsWith(getConnectStringPrefix())) {
      return null;
    }
    // Connection string properties are given precedence over config properties.
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
      rootSchema.add(CATALOG_NAME, createPinotSchema(properties));
      return connection;
    } catch (IOException e) {
      throw new SQLTransientConnectionException("Problem loading " + url, e);
    } catch (Exception e) {
      throw new SQLNonTransientException("Problem loading " + url, e);
    }
  }

  protected PinotSchema createPinotSchema(Properties properties) {
    return new PinotSchema(createClient(properties));
  }

  /** Builds the {@link PinotClient} for the connection. Override to route through a managed plane. */
  protected PinotClient createClient(Properties properties) {
    return new PinotControllerClient(properties.getProperty(CONTROLLER_URL));
  }
}
