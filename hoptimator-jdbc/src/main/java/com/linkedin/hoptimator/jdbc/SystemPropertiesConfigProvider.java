package com.linkedin.hoptimator.jdbc;

import java.util.Properties;

import com.linkedin.hoptimator.ConfigProvider;

public class SystemPropertiesConfigProvider implements ConfigProvider {

  public Properties loadConfig(String namespace) {
    return System.getProperties();
  }
}
