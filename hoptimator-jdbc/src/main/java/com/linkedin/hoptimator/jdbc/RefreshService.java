package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.RefreshProvider;
import com.linkedin.hoptimator.RefreshTarget;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;


/**
 * Resolves {@code REFRESH} targets by delegating to the pluggable {@link RefreshProvider}s
 * registered by the active backend(s). The DDL layer asks "what are the triggers immediately
 * upstream of this object, and what kind of object is it?" without knowing how the backend wires
 * triggers to their downstream materialized view or logical table.
 */
public final class RefreshService {

  private RefreshService() {
  }

  public static Collection<RefreshProvider> providers() {
    ServiceLoader<RefreshProvider> loader = ServiceLoader.load(RefreshProvider.class);
    List<RefreshProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(providers::add);
    return providers;
  }

  /**
   * Resolves the refresh target for {@code path}, or returns {@code null} when no registered
   * provider recognizes the object (e.g. it isn't a materialized view or logical table, or no
   * backend is configured). The first provider to recognize the object wins.
   */
  public static RefreshTarget resolve(List<String> path, Connection connection) throws SQLException {
    for (RefreshProvider provider : providers()) {
      RefreshTarget target = provider.resolve(path, connection);
      if (target != null) {
        return target;
      }
    }
    return null;
  }
}
