package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;


/**
 * Pre-delete dependency check, run by the validator framework when a {@link Source} is wrapped
 * in {@link com.linkedin.hoptimator.PendingDelete}. Delegates to the existing
 * {@link PipelineDependencyChecker} (label-selector + annotation collision-guard + self-uid
 * exclusion) and surfaces any blocking pipeline as a validation error rather than throwing
 * directly — that's what the validator contract calls for.
 */
final class K8sPipelineDependencyValidator implements Validator {

  private final Source source;
  private final String selfOwnerUid;

  K8sPipelineDependencyValidator(Source source, String selfOwnerUid) {
    this.source = source;
    this.selfOwnerUid = selfOwnerUid;
  }

  @Override
  public void validate(Issues issues, Connection connection) {
    if (connection == null) {
      issues.error("Cannot run pre-delete dependency check without a connection");
      return;
    }
    try {
      PipelineDependencyChecker.assertNoExternalDependents(
          K8sContext.create(connection), source.database(), source.path(), selfOwnerUid);
    } catch (SQLException e) {
      issues.error(e.getMessage());
    }
  }
}
