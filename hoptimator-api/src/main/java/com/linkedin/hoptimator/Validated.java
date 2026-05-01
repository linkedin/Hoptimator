package com.linkedin.hoptimator;

import java.sql.Connection;


public interface Validated {

  /**
   * Validates {@code this}, recording any problems in {@code issues}. The connection is always
   * supplied so validators can run lookups against external systems (e.g. pre-delete dependency
   * checks). Pass {@code null} only when the caller genuinely has no connection — most validators
   * ignore it, but some require it.
   */
  void validate(Validator.Issues issues, Connection connection);
}
