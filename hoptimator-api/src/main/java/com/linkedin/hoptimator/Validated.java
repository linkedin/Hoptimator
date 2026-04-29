package com.linkedin.hoptimator;

import java.sql.Connection;


public interface Validated {

  void validate(Validator.Issues issues);

  /**
   * Connection-aware validation hook. Default delegates to {@link #validate(Validator.Issues)}.
   * Override (or have a {@link ValidatorProvider} return a Validator that overrides) when the
   * check needs to query an external system — e.g. pre-delete dependency lookups.
   */
  default void validate(Validator.Issues issues, Connection connection) {
    validate(issues);
  }
}
