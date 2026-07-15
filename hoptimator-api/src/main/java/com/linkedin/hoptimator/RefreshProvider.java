package com.linkedin.hoptimator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;


/**
 * Resolves a {@code REFRESH} target: given the path of a materialized view or logical table, reports
 * what kind of object it is and the names of the triggers immediately upstream of it (the triggers a
 * REFRESH should fire to backfill it).
 *
 * <p>This is the seam that keeps the DDL layer decoupled from the backend. A backend (e.g.
 * Kubernetes) discovers the upstream triggers however it likes — owner references, dependency
 * labels, etc. — and exposes them here, so {@code hoptimator-jdbc} never touches the backend API.
 */
public interface RefreshProvider {

  /**
   * Resolves the refresh target for {@code path}, or returns {@code null} when this provider does
   * not recognize the object (it isn't a materialized view or logical table this backend manages).
   * Returning a {@link RefreshTarget} with an empty trigger list means "the object exists but has no
   * upstream triggers" — a distinct, non-null outcome the caller treats as an error.
   */
  RefreshTarget resolve(List<String> path, Connection connection) throws SQLException;
}
