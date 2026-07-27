package com.linkedin.hoptimator.jdbc;

import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;


/**
 * Minimal registry-native {@link DatabaseConfigResolverProvider} for {@code hoptimator-jdbc} unit
 * tests, which run without a real registry (e.g. hoptimator-k8s) on the classpath. It resolves the
 * database identifier to the path's schema segment and reports no per-{@code Database} config, which
 * is enough to exercise {@link TableService}'s Avro handling, guards, and dry-run rendering without
 * standing up a backend.
 */
public final class TestDatabaseConfigResolverProvider implements DatabaseConfigResolverProvider {

  @Override
  public DatabaseConfigResolver resolver(Properties connectionProperties) {
    return new DatabaseConfigResolver() {
      @Override
      public @Nullable Properties databaseProperties(@Nullable String catalog, @Nullable String schema,
          String connectionPrefix) {
        return null;
      }

      @Override
      public String databaseName(List<String> tablePath) {
        return tablePath.get(tablePath.size() - 2);
      }
    };
  }
}
