package com.linkedin.hoptimator.pinot;

import com.linkedin.hoptimator.jdbc.CalciteDriver;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientConnectionException;
import java.util.Properties;
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
  protected void configureConnection(CalciteConnection connection, String url, Properties properties) throws SQLException {
    connection.setAutoCommit(true); // to prevent rollback()
    connection.setCatalog(CATALOG_NAME);
    SchemaPlus rootSchema = connection.getRootSchema();
    PinotSchema schema = createPinotSchema(properties);
    addConnectionResource(connection, schema);
    rootSchema.add(CATALOG_NAME, schema);
  }

  @Override
  protected SQLException connectionException(String url, Exception failure) {
    if (failure instanceof IOException) {
      return new SQLTransientConnectionException("Problem loading " + url, failure);
    }
    return new SQLNonTransientException("Problem loading " + url, failure);
  }

  protected PinotSchema createPinotSchema(Properties properties) {
    return new PinotSchema(createClient(properties));
  }

  /** Builds the {@link PinotClient} for the connection. Override to route through a managed plane. */
  protected PinotClient createClient(Properties properties) {
    return new PinotControllerClient(properties.getProperty(CONTROLLER_URL));
  }
}
