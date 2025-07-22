package com.linkedin.hoptimator.jdbc;

import java.sql.Connection;
import java.util.Properties;

import com.linkedin.hoptimator.ConfigProvider;

public class SystemPropertiesConfigProvider implements ConfigProvider {

  public Properties loadConfig(Connection connection) {
    return System.getProperties();
  }
}
