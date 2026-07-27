package com.linkedin.hoptimator.jdbc;

import java.util.Comparator;
import java.util.Properties;
import java.util.ServiceLoader;


/** Discovers the registry-native {@link DatabaseConfigResolver} to use. */
public final class DatabaseConfigResolvers {

  private DatabaseConfigResolvers() {
  }

  /**
   * Returns the highest-priority service-loaded {@link DatabaseConfigResolver}, built from raw
   * connection properties. A registry-native provider — e.g. the {@code hoptimator-k8s} one — must
   * be on the classpath; there is no Calcite-based fallback, so config resolution is identical on
   * the SQL and connection-free direct paths.
   */
  public static DatabaseConfigResolver forProperties(Properties connectionProperties) {
    return ServiceLoader.load(DatabaseConfigResolverProvider.class).stream()
        .map(ServiceLoader.Provider::get)
        .max(Comparator.comparingInt(DatabaseConfigResolverProvider::priority))
        .map(provider -> provider.resolver(connectionProperties))
        .orElseThrow(() -> new IllegalStateException(
            "No DatabaseConfigResolverProvider registered; a registry-native resolver "
                + "(e.g. hoptimator-k8s) must be on the classpath."));
  }
}
