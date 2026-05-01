package com.linkedin.hoptimator;

import java.sql.Connection;


public interface Validated {

  /**
   * Validates {@code this}, recording any problems in {@code issues}. The connection is always
   * supplied so validators can run lookups against external systems (e.g. pre-delete dependency
   * checks).
   */
  void validate(Validator.Issues issues, Connection connection);
}
