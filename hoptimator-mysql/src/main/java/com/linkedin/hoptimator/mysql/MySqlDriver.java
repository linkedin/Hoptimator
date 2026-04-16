package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.jdbc.CalciteDriver;
import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.calcite.avatica.AvaticaConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Properties;

/**
 * JDBC driver for MySQL databases.
 *
 * <p>Overrides {@link CalciteDriver#createRootSchema} to supply a {@link MySqlRootSchema}
 * that lazily discovers MySQL databases as sub-schemas — no connection to MySQL is opened
 * at connect time. Tables within each database are also loaded lazily via
 * {@link com.linkedin.hoptimator.jdbc.schema.LazyLookup}.
 */
public class MySqlDriver extends CalciteDriver {
  public static final String CATALOG_NAME = "MYSQL";
  public static final String CONNECTION_PREFIX = "jdbc:mysql-hoptimator://";

  private static final Logger log = LoggerFactory.getLogger(MySqlDriver.class);

  static {
    new MySqlDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECTION_PREFIX;
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(this.getClass(), "mysql.properties", "mysql", "0", "venice", "0");
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
      Connection connection = super.connect(url, props); // createRootSchema() called here
      if (connection == null) {
        throw new IOException("Could not connect to " + url);
      }
      connection.setAutoCommit(true); // to prevent rollback()
      connection.setCatalog(CATALOG_NAME);

      String mySqlUrl = properties.getProperty("url");
      if (mySqlUrl == null) {
        throw new SQLException("Missing required parameter 'url' for MySQL connection");
      }

      return connection;
    } catch (IOException e) {
      throw new SQLNonTransientException("Problem loading " + url, e);
    }
  }

  /**
   * Skip the lattice scan that Calcite normally performs at connection init time.
   * MySqlDriver does not define lattices or use schema model files, so the scan
   * only causes eager MySQL schema loading with no benefit.
   */
  @Override
  protected void onConnectionInit(AvaticaConnection connection) {
    // no-op: lattice scan skipped intentionally to prevent eager loading of all schemas
  }

  @Override
  protected CalciteSchema createRootSchema(boolean cache, Properties properties) {
    AbstractSchema rootSchema = createMySqlRootSchema(properties);
    return CalciteSchema.createRootSchema(false, cache, "", rootSchema);
  }

  protected AbstractSchema createMySqlRootSchema(Properties properties) {
    return new MySqlRootSchema(properties);
  }
}
