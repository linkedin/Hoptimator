package com.linkedin.hoptimator.jdbc;

import java.util.Comparator;
import java.util.Properties;
import java.util.ServiceLoader;


/** Discovers the {@link DatabaseConfigResolver} to use, preferring a registry-native provider. */
public final class DatabaseConfigResolvers {

  private DatabaseConfigResolvers() {
  }

  /**
   * Returns the highest-priority service-loaded {@link DatabaseConfigResolver} built from the
   * connection's properties, or a {@link CalciteDatabaseConfigResolver} over the connection when no
   * provider is registered.
   */
  public static DatabaseConfigResolver forConnection(HoptimatorConnection connection) {
    return ServiceLoader.load(DatabaseConfigResolverProvider.class).stream()
        .map(ServiceLoader.Provider::get)
        .max(Comparator.comparingInt(DatabaseConfigResolverProvider::priority))
        .map(provider -> provider.resolver(connection.connectionProperties()))
        .orElseGet(() -> new CalciteDatabaseConfigResolver(connection));
  }

  /**
   * Returns the highest-priority service-loaded {@link DatabaseConfigResolver} built from raw
   * connection properties, for the connection-free direct path. Unlike {@link #forConnection}, there
   * is no Calcite fallback (that would need a connection), so a registry-native provider — e.g. the
   * K8s one — must be on the classpath.
   */
  public static DatabaseConfigResolver forProperties(Properties connectionProperties) {
    return ServiceLoader.load(DatabaseConfigResolverProvider.class).stream()
        .map(ServiceLoader.Provider::get)
        .max(Comparator.comparingInt(DatabaseConfigResolverProvider::priority))
        .map(provider -> provider.resolver(connectionProperties))
        .orElseThrow(() -> new IllegalStateException(
            "No DatabaseConfigResolverProvider registered; the direct API requires a registry-native "
                + "resolver (e.g. hoptimator-k8s) on the classpath."));
  }
}
