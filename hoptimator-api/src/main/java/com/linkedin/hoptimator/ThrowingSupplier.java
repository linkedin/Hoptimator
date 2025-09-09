package com.linkedin.hoptimator;

import java.sql.SQLException;


/**
 * Functional interface that allows suppliers to throw SQLException.
 */
@FunctionalInterface
public interface ThrowingSupplier<T> {
  T get() throws SQLException;
}
