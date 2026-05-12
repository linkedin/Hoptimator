package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.Nullable;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;


/**
 * Pre-delete dependency check, run by the validator framework when a {@link Source} is wrapped
 * in {@link com.linkedin.hoptimator.PendingDelete}. Delegates to the existing
 * {@link DependencyChecker} (label-selector + annotation collision-guard + self-owner
 * exclusion) and surfaces any blocking pipeline as a validation error.
 */
final class K8sDependencyValidator implements Validator {

  private final Source source;
  private final String selfOwnerKind;
  private final String selfOwnerName;

  K8sDependencyValidator(Source source, @Nullable String selfOwnerKind, @Nullable String selfOwnerName) {
    this.source = source;
    this.selfOwnerKind = selfOwnerKind;
    this.selfOwnerName = selfOwnerName;
  }

  @Override
  public void validate(Issues issues, Connection connection) {
    try {
      DependencyChecker.assertNoExternalDependents(
          K8sContext.create(connection), source.database(), source.path(),
          selfOwnerKind, selfOwnerName);
    } catch (SQLException e) {
      issues.error(e.getMessage());
    }
  }
}
