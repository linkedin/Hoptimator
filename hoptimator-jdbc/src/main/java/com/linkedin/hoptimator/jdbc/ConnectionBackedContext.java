package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.DeploymentContext;


/**
 * A {@link DeploymentContext} that is backed by a Calcite {@link HoptimatorConnection}. Both the
 * SQL path ({@link CalciteDeploymentContext}) and the direct API path
 * ({@link DirectDeploymentContext}) resolve {@code Database} config from the same connection; they
 * differ only in how the <em>table's</em> row type is resolved. Consumers that genuinely need the
 * connection (e.g. to read Database CRD config) should depend on this interface rather than casting
 * to a specific implementation.
 */
public interface ConnectionBackedContext extends DeploymentContext {

  /** The underlying connection, used for Database-registry/config resolution (not table schema). */
  HoptimatorConnection connection();
}
