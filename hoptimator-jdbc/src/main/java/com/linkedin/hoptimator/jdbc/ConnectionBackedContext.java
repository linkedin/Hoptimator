package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.DeploymentContext;

import javax.annotation.Nullable;


/**
 * A {@link DeploymentContext} that is backed by a Calcite {@link HoptimatorConnection}. Only the
 * SQL path ({@link CalciteDeploymentContext}) is connection-backed; the direct API path
 * ({@link DirectDeploymentContext}) holds no connection. Consumers that genuinely need the
 * connection should depend on this interface (or use {@link #connectionOrNull}) rather than casting
 * to a specific implementation, so the dependency is explicit and degrades to {@code null} on the
 * connection-free path.
 */
public interface ConnectionBackedContext extends DeploymentContext {

  /** The underlying connection, used for Database-registry/config resolution (not table schema). */
  HoptimatorConnection connection();

  /**
   * The connection backing {@code context}, or {@code null} when it is not connection-backed (e.g.
   * the direct API path). Lets a caller opt into the connection only where one exists.
   */
  static @Nullable HoptimatorConnection connectionOrNull(@Nullable DeploymentContext context) {
    return context instanceof ConnectionBackedContext ? ((ConnectionBackedContext) context).connection() : null;
  }
}
