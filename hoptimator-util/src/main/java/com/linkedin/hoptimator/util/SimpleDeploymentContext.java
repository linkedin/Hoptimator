package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.DeploymentContext;

import javax.annotation.Nullable;
import java.util.Properties;


/**
 * A minimal {@link DeploymentContext} that carries only connection-level {@link #properties()} and
 * resolves no per-{@code Database} config. Intended for control-plane callers — operators,
 * reconcilers, event processors — that only need to run {@link ConfigService} / {@code K8sContext}
 * off a plain {@link Properties} bag (a namespace, K8s access config, hints), without a Calcite
 * connection or a {@code Database} registry.
 *
 * <p>{@link #databaseProperties} always returns {@code null}: these callers never resolve database
 * connection URLs, so there is no need to wrap a dummy {@code HoptimatorConnection} in a
 * {@code CalciteDeploymentContext} just to shuttle properties. Use {@code CalciteDeploymentContext}
 * (SQL path) or {@code DirectDeploymentContext} (direct API path) when database config is required.
 */
public final class SimpleDeploymentContext implements DeploymentContext {

  private final Properties properties;

  public SimpleDeploymentContext(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Properties properties() {
    return properties;
  }

  @Override
  public @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
      String connectionPrefix) {
    return null;
  }
}
