package com.linkedin.hoptimator;


public interface Validated {

  /**
   * Validates {@code this}, recording any problems in {@code issues}. The context is always
   * supplied so validators can run lookups against external systems (e.g. pre-delete dependency
   * checks).
   */
  void validate(Validator.Issues issues, DeploymentContext context);
}
