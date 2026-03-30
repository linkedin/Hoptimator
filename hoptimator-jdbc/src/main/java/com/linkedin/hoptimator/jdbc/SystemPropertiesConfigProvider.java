package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.ConfigProvider;

import java.sql.Connection;
import java.util.Properties;

public class SystemPropertiesConfigProvider implements ConfigProvider {

  public Properties loadConfig(Connection connection) {
    return System.getProperties();
  }
}
