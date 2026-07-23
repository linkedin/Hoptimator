package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.List;


/** Deploys something. */
public interface Deployer {

  void create() throws SQLException;

  void delete() throws SQLException;

  void update() throws SQLException;

  /**
   * Whether the backing resource this deployer manages already exists. Used by the connection-free
   * direct path to enforce {@code CREATE} (not {@code OR REPLACE}) semantics — the SQL/DDL path
   * enforces the same thing via the Calcite catalog before deployers run, so this is not consulted
   * there. Implementations must report their backing resource's existence truthfully (rather than
   * assume-absent), so a re-create against a different schema is rejected instead of silently masked.
   */
  boolean exists() throws SQLException;

  /** Render a list of specs, usually YAML. */
  List<String> specify() throws SQLException;

  /** Deployers are expected to track the state of changes made and revert them on demand. */
  void restore();
}
