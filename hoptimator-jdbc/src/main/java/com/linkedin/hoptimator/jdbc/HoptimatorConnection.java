package com.linkedin.hoptimator.jdbc;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;

import com.linkedin.hoptimator.util.DelegatingConnection;


public class HoptimatorConnection extends DelegatingConnection {

  private final CalciteConnection connection;
  private final Properties connectionProperties;

  public HoptimatorConnection(CalciteConnection connection, Properties connectionProperties) {
    super(connection);
    this.connection = connection;
    this.connectionProperties = connectionProperties;
  }

  @Override
  public Statement createStatement() throws SQLException {
    return connection.createStatement(); 
  }

  public Properties connectionProperties() {
    return connectionProperties;
  }

  public CalcitePrepare.Context createPrepareContext() {
    return connection.createPrepareContext(); 
  }

  public CalciteConnection calciteConnection() {
    return connection;
  }
}
