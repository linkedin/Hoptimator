package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;

import com.linkedin.hoptimator.PendingDelete;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;


/**
 * Returns Kubernetes-backed validators that need a connection. Currently produces a single
 * dependency-guard validator scoped to {@link PendingDelete} of a {@link Source}: at DROP TABLE
 * time we scan existing Pipeline CRDs to make sure none still reference the resource being
 * deleted.
 *
 * <p>Keying off {@code PendingDelete} (not raw {@code Source}) is intentional. An unrelated
 * future caller of {@code ValidationService.validateOrThrow(source, connection)} should not
 * accidentally trigger a pre-delete check; only callers that wrap with {@code PendingDelete}
 * are signalling delete intent.
 *
 * <p>The connection-less {@link #validators(Object)} overload returns nothing — these checks
 * are inherently external and can't run without a connection.
 */
public class K8sValidatorProvider implements ValidatorProvider {

  @Override
  public <T> Collection<Validator> validators(T obj) {
    return Collections.emptyList();
  }

  @Override
  public <T> Collection<Validator> validators(T obj, Connection connection) {
    if (connection == null) {
      return Collections.emptyList();
    }
    if (obj instanceof PendingDelete) {
      Object target = ((PendingDelete<?>) obj).target();
      if (target instanceof Source) {
        return Collections.singletonList(
            new K8sPipelineDependencyValidator((Source) target, connection));
      }
    }
    return Collections.emptyList();
  }
}
