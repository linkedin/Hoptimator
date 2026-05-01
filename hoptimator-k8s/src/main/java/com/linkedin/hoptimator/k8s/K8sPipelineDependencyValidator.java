package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.Nullable;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;


/**
 * Pre-delete dependency check, run by the validator framework when a {@link Source} is wrapped
 * in {@link com.linkedin.hoptimator.PendingDelete}. Delegates to the existing
 * {@link PipelineDependencyChecker} (label-selector + annotation collision-guard + self-uid
 * exclusion) and surfaces any blocking pipeline as a validation error
 */
final class K8sPipelineDependencyValidator implements Validator {

  private final Source source;
  private final String selfOwnerUid;

  K8sPipelineDependencyValidator(Source source, @Nullable String selfOwnerUid) {
    this.source = source;
    this.selfOwnerUid = selfOwnerUid;
  }

  @Override
  public void validate(Issues issues, Connection connection) {
    try {
      PipelineDependencyChecker.assertNoExternalDependents(
          K8sContext.create(connection), source.database(), source.path(), selfOwnerUid);
    } catch (SQLException e) {
      issues.error(e.getMessage());
    }
  }
}
