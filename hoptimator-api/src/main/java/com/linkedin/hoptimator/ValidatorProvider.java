package com.linkedin.hoptimator;

import java.sql.Connection;
import java.util.Collection;


public interface ValidatorProvider {

  /**
   * Returns validators that should be applied to {@code obj}.
   */
  <T> Collection<Validator> validators(T obj, Connection connection);
}
