package com.linkedin.hoptimator.util;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;
import javax.sql.DataSource;


/** DataSource which loads a driver by URL, and papers over features the driver may lack. */
public class DelegatingDataSource implements DataSource {
  private static final Logger logger = Logger.getLogger("DelegatingDataSource");

  private String url;
  private int loginTimeout = 60;
  private PrintWriter printWriter = new PrintWriter(System.out, false, StandardCharsets.UTF_8);

  public DelegatingDataSource() {
  }

  public void setUrl(String url) {
    this.url = url;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return new DelegatingConnection(DriverManager.getConnection(url));
  }

  @Override
  public Connection getConnection(String user, String pass) throws SQLException {
    return new DelegatingConnection(DriverManager.getConnection(url, user, pass));
  }

  @Override
  public int getLoginTimeout() {
    return loginTimeout;
  }

  @Override
  public void setLoginTimeout(int timeout) {
    this.loginTimeout = timeout;
  }

  @Override
  public PrintWriter getLogWriter() {
    return printWriter;
  }

  @Override
  public void setLogWriter(PrintWriter printWriter) {
    this.printWriter = printWriter;
  }

  @Override
  public Logger getParentLogger() {
    return logger;
  }

  @Override
  public boolean isWrapperFor(Class<?> clazz) throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> clazz) throws SQLException {
    return null;
  }
}
