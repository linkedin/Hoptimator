package com.linkedin.hoptimator.jdbc;

import java.util.Properties;


/**
 * Service-loaded factory for a {@link DatabaseConfigResolver}. This lets a registry-native module
 * (e.g. {@code hoptimator-k8s}) supply a resolver that reads {@code Database} config directly from
 * its source — inverting the dependency so the connection-free direct path in
 * {@code hoptimator-jdbc} can resolve config without a Calcite catalog.
 *
 * <p>The highest-{@link #priority()} provider wins; when none is registered, callers fall back to
 * {@link CalciteDatabaseConfigResolver}.
 */
public interface DatabaseConfigResolverProvider {

  /**
   * Builds a resolver from connection-level properties (e.g. {@code k8s.*} config needed to reach
   * the registry).
   */
  DatabaseConfigResolver resolver(Properties connectionProperties);

  /** Higher wins. Default {@code 0}. */
  default int priority() {
    return 0;
  }
}
