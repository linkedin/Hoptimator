package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;

import com.linkedin.hoptimator.PendingDelete;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;


/**
 * Returns Kubernetes-backed validators. Currently produces a single dependency-guard validator
 * scoped to {@link PendingDelete} of a {@link Source}: at DROP TABLE time we scan existing
 * Pipeline CRDs to make sure none still reference the resource being deleted.
 *
 * <p>Keying off {@code PendingDelete} (not raw {@code Source}) is intentional — an unrelated
 * caller of {@code ValidationService.validateOrThrow(source, connection)} should not
 * accidentally trigger a pre-delete check; only callers wrapping with {@code PendingDelete}
 * are signalling delete intent.
 */
public class K8sValidatorProvider implements ValidatorProvider {

  @Override
  public <T> Collection<Validator> validators(T obj, Connection connection) {
    if (obj instanceof PendingDelete) {
      Object target = ((PendingDelete<?>) obj).target();
      if (target instanceof Source) {
        return Collections.singletonList(
            new K8sPipelineDependencyValidator((Source) target));
      }
    }
    return Collections.emptyList();
  }
}
