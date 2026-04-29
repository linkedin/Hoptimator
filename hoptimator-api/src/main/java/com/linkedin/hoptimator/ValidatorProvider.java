package com.linkedin.hoptimator;

import java.sql.Connection;
import java.util.Collection;


public interface ValidatorProvider {

  <T> Collection<Validator> validators(T obj);

  /**
   * Connection-aware variant. Default delegates to {@link #validators(Object)}. Implementations
   * that need to capture the connection at construction (so the resulting Validators can run
   * lookups against an external system) override this overload.
   */
  default <T> Collection<Validator> validators(T obj, Connection connection) {
    return validators(obj);
  }
}
