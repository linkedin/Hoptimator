package com.linkedin.hoptimator.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;


final class ConnectionResources implements AutoCloseable {
  private final List<AutoCloseable> resources = new ArrayList<>();
  private boolean closed;

  synchronized void add(AutoCloseable resource) throws SQLException {
    requireNonNull(resource, "resource");
    if (closed) {
      SQLException failure = new SQLException("Connection resources are already closed");
      try {
        resource.close();
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        failure.addSuppressed(e);
      }
      throw failure;
    }
    if (resources.stream().noneMatch(existing -> existing == resource)) {
      resources.add(resource);
    }
  }

  @Override
  public void close() throws SQLException {
    List<AutoCloseable> owned;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      owned = new ArrayList<>(resources);
      resources.clear();
    }
    SQLException failure = null;
    for (int i = owned.size() - 1; i >= 0; i--) {
      try {
        owned.get(i).close();
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        if (failure == null) {
          failure = new SQLException("Failed to close connection resources", e);
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
