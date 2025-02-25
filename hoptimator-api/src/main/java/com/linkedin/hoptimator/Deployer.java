package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.List;


/** Deploys something. */
public interface Deployer {

  void create() throws SQLException;

  void delete() throws SQLException;

  void update() throws SQLException;

  /** Render a list of specs, usually YAML. */
  List<String> specify() throws SQLException;
}
