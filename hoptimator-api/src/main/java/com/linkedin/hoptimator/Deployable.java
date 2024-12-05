package com.linkedin.hoptimator;

import java.util.List;
import java.sql.SQLException;

public interface Deployable {

  void create() throws SQLException;
  void delete() throws SQLException;
  void update() throws SQLException;

  /** Render a list of specs, usually YAML. */
  List<String> specify() throws SQLException;
}
