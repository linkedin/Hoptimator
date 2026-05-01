package com.linkedin.hoptimator;

import java.sql.Connection;
import java.util.Collection;


public interface ValidatorProvider {

  /**
   * Returns validators that should be applied to {@code obj}. The connection is always supplied
   * — providers that don't need it can ignore it (matches the {@code DeployerProvider} pattern).
   * Implementations capturing the connection in returned validators must accept that the
   * connection may be {@code null} when the caller has none.
   */
  <T> Collection<Validator> validators(T obj, Connection connection);
}
