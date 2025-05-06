package com.linkedin.hoptimator.demodb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.ConnectStringParser;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.SchemaPlus;


/** JDBC driver with fake in-memory data. */
public class DemoDriver extends Driver {

  static {
    new DemoDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return "jdbc:demodb://";
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return DriverVersion.load(this.getClass(), "demodb.properties", "demodb", "0", "demodb", "0");
  }

  @Override
  public Connection connect(String url, Properties props) throws SQLException {
    if (!url.startsWith(getConnectStringPrefix())) {
      return null;
    }
    Properties properties = new Properties();
    properties.putAll(props);
    properties.putAll(ConnectStringParser.parse(url.substring(getConnectStringPrefix().length())));

    Set<String> schemas = Arrays.stream(properties.getProperty("names").split(","))
        .map(String::trim)
        .filter(x -> !x.isEmpty())
        .map(x -> x.toUpperCase(Locale.ROOT))
        .collect(Collectors.toSet());
    try {
      Connection connection = super.connect(url, properties);
      if (connection == null) {
        throw new IOException("Could not connect to " + url);
      }
      connection.setAutoCommit(true); // to prevent rollback()
      CalciteConnection calciteConnection = (CalciteConnection) connection;
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      if (schemas.isEmpty() || schemas.contains("PROFILE")) {
        rootSchema.add("PROFILE", new ProfileSchema());
      }
      if (schemas.isEmpty() || schemas.contains("ADS")) {
        rootSchema.add("ADS", new AdsSchema());
      }
      return connection;
    } catch (IOException e) {
      throw new SQLTransientException("Problem loading " + url, e);
    } catch (Exception e) {
      throw new SQLNonTransientException("Problem loading " + url, e);
    }

  }
}
