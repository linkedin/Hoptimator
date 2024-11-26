package com.linkedin.hoptimator;

import java.sql.Wrapper;
import java.sql.SQLException;

/** Registers a set of tables, possibly within schemas and sub-schemas. */
public interface Catalog {

  String name();
  String description();
  void register(Wrapper parentSchema) throws SQLException;
}
