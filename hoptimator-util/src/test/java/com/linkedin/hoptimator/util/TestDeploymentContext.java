package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.DeploymentContext;

import java.util.Properties;

import javax.annotation.Nullable;


/** Minimal no-op {@link DeploymentContext} for unit tests that don't exercise config resolution. */
public class TestDeploymentContext implements DeploymentContext {

  private final Properties properties;

  public TestDeploymentContext() {
    this(new Properties());
  }

  public TestDeploymentContext(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Properties properties() {
    return properties;
  }

  @Override
  public Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
      String connectionPrefix) {
    return null;
  }
}
