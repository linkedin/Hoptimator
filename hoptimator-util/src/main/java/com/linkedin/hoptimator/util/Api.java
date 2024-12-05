package com.linkedin.hoptimator.util;

import java.util.Collection;
import java.sql.SQLException;

/** A set of CRUD'able objects. */
public interface Api<T> {
  
  Collection<T> list() throws SQLException;

  default void create(T t) throws SQLException {
    throw new UnsupportedOperationException("This API is read-only.");
  }

  default void delete(T t) throws SQLException {
    throw new UnsupportedOperationException("This API is read-only.");
  }

  default void update(T t) throws SQLException {
    throw new UnsupportedOperationException("This API is read-only.");
  }
}
