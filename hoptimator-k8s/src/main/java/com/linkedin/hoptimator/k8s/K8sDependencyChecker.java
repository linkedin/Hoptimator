package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;

import com.linkedin.hoptimator.DependencyChecker;
import com.linkedin.hoptimator.Source;


/**
 * {@link DependencyChecker} implementation backed by Kubernetes: queries Pipeline CRDs via a
 * label selector and delegates to {@link PipelineDependencyChecker} for the actual match +
 * collision-guard logic.
 *
 * <p>Registered via {@code META-INF/services/com.linkedin.hoptimator.DependencyChecker} so
 * {@code DeploymentService.delete} picks it up automatically wherever this module is on the
 * classpath.
 */
public class K8sDependencyChecker implements DependencyChecker {

  @Override
  public void assertNoExternalDependents(Connection connection, Source source, String selfOwnerUid)
      throws SQLException {
    if (connection == null) {
      return;
    }
    K8sContext context = K8sContext.create(connection);
    PipelineDependencyChecker.assertNoExternalDependents(
        context, source.database(), source.path(), selfOwnerUid);
  }
}
