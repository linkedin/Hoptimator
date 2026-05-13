package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;

import com.linkedin.hoptimator.PendingDelete;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;


/**
 * Returns a Kubernetes-backed dependency-guard validator for any {@link Source} wrapped in a
 * {@link PendingDelete} — i.e. when a DROP is issued. Keying off
 * {@code PendingDelete<Source>} (not raw {@code Source}) makes the guard explicitly delete-time:
 * other callers of {@code ValidationService.validateOrThrow(source, connection)} won't trigger
 * a pre-delete lookup against K8s.
 */
public class K8sValidatorProvider implements ValidatorProvider {

  @Override
  public <T> Collection<Validator> validators(T obj, Connection connection) {
    if (obj instanceof PendingDelete) {
      PendingDelete<?> pd = (PendingDelete<?>) obj;
      Object target = pd.target();
      if (target instanceof Source) {
        return Collections.singletonList(
            new K8sPipelineDependencyValidator((Source) target, pd.selfOwnerKind(), pd.selfOwnerName()));
      }
    }
    return Collections.emptyList();
  }
}
