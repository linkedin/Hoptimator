package com.linkedin.hoptimator;

import java.sql.Connection;
import java.sql.SQLException;


/**
 * SPI for verifying that no external resource (typically a Pipeline) still depends on a
 * {@link DependencyGuarded} deployer's managed resource before it is deleted. Loaded by
 * {@code DeploymentService.delete} via {@link java.util.ServiceLoader} — backend modules
 * (e.g. {@code hoptimator-k8s}) provide an implementation, and backend-agnostic deployer
 * modules (e.g. {@code hoptimator-kafka}) depend only on {@code hoptimator-api}.
 *
 * <p>Implementations must throw {@link SQLException} with a descriptive message naming the
 * blocking dependents when deletion should be rejected.
 */
public interface DependencyChecker {

  /**
   * Throws {@link SQLException} if any external dependent still references {@code source}.
   *
   * @param connection     the JDBC connection in whose context the check should run; the
   *                       implementation derives any further context (K8s namespace, cluster,
   *                       etc.) from here
   * @param source         the resource being guarded
   * @param selfOwnerUid   UID of an owner CRD whose owned dependents are exempt; may be null
   */
  void assertNoExternalDependents(Connection connection, Source source, String selfOwnerUid)
      throws SQLException;
}
