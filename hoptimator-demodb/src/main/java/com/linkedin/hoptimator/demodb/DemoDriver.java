package com.linkedin.hoptimator.demodb;

import com.linkedin.hoptimator.jdbc.CalciteDriver;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/** JDBC driver with fake in-memory data. */
public class DemoDriver extends CalciteDriver {

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
  protected Properties connectionProperties(String url, Properties info) throws SQLException {
    Properties properties = super.connectionProperties(url, info);
    requireNonNull(properties.getProperty("names"));
    return properties;
  }

  @Override
  protected void configureConnection(CalciteConnection connection, String url, Properties properties) throws SQLException {
    Set<String> schemas = Arrays.stream(properties.getProperty("names").split(","))
        .map(String::trim)
        .filter(x -> !x.isEmpty())
        .map(x -> x.toUpperCase(Locale.ROOT))
        .collect(Collectors.toSet());
    connection.setAutoCommit(true); // to prevent rollback()
    SchemaPlus rootSchema = connection.getRootSchema();
    if (schemas.isEmpty() || schemas.contains("PROFILE")) {
      rootSchema.add("PROFILE", new ProfileSchema());
    }
    if (schemas.isEmpty() || schemas.contains("ADS")) {
      rootSchema.add("ADS", new AdsSchema());
    }
  }

  @Override
  protected SQLException connectionException(String url, Exception failure) {
    if (failure instanceof IOException) {
      return new SQLTransientException("Problem loading " + url, failure);
    }
    return new SQLNonTransientException("Problem loading " + url, failure);
  }
}
