package com.linkedin.hoptimator.mysql;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Properties;

import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC driver for MySQL databases.
 */
public class MySqlDriver extends Driver {
  private static final String CATALOG_NAME = "MYSQL";

  private static final Logger log = LoggerFactory.getLogger(MySqlDriver.class);

  static {
    new MySqlDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return "jdbc:mysql-hoptimator://";
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
      Connection connection = super.connect(url, props);
      if (connection == null) {
        throw new IOException("Could not connect to " + url);
      }
      connection.setAutoCommit(true); // to prevent rollback()
      connection.setCatalog(CATALOG_NAME);

      CalciteConnection calciteConnection = (CalciteConnection) connection;
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      String mySqlUrl = properties.getProperty("url");
      if (mySqlUrl == null) {
        throw new SQLException("Missing required parameter 'url' for MySQL connection");
      }
      String user = properties.getProperty("user", "");
      String password = properties.getProperty("password", "");

      try (Connection conn = DriverManager.getConnection(mySqlUrl, user, password)) {
        DatabaseMetaData metaData = conn.getMetaData();
        // MySQL catalogs are the databases
        try (ResultSet rs = metaData.getCatalogs()) {
          while (rs.next()) {
            String schemaName = rs.getString("TABLE_CAT");
            TableSchema tableSchema = new TableSchema(properties, schemaName);
            tableSchema.populate();
            rootSchema.add(schemaName, tableSchema);
            log.debug("Registered MySQL schema: {}", schemaName);
          }
        }
      }

      return connection;
    } catch (IOException e) {
      throw new SQLNonTransientException("Problem loading " + url, e);
    }
  }
}
