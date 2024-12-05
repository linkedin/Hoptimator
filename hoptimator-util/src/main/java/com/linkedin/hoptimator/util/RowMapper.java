package com.linkedin.hoptimator.util;

public interface RowMapper<T, U> {

  U toRow(T t);
  T fromRow(U u);
}
