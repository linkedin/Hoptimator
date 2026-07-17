package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.ConfigProvider;
import com.linkedin.hoptimator.DeploymentContext;

import java.util.Properties;

public class SystemPropertiesConfigProvider implements ConfigProvider {

  public Properties loadConfig(DeploymentContext context) {
    return System.getProperties();
  }
}
