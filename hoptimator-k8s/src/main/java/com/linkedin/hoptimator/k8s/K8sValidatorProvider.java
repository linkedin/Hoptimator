package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.ValidatorProvider;


/**
 * Returns Kubernetes-backed validators that need a connection. Currently produces a single
 * dependency-guard validator for {@link Source}: at DROP TABLE time it scans existing
 * Pipeline CRDs to make sure none still reference the resource being deleted.
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
    if (obj instanceof Source) {
      return Collections.singletonList(
          new K8sPipelineDependencyValidator((Source) obj, K8sContext.create(connection)));
    }
    return Collections.emptyList();
  }
}
