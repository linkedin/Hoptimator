package com.linkedin.hoptimator.jdbc;

import java.util.Comparator;
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
}
