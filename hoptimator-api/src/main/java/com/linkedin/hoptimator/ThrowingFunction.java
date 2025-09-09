package com.linkedin.hoptimator;

import java.sql.SQLException;


/**
 * Functional interface that allows functions to throw SQLException.
 */
@FunctionalInterface
public interface ThrowingFunction<T, R> {
  R apply(T t) throws SQLException;
}