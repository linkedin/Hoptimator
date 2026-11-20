package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.Deployer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;


/** Owns deployers until validation, deployment and any rollback have all completed. */
public final class DeployerScope implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DeployerScope.class);

  private List<Deployer> deployers;
  private boolean closed;

  public DeployerScope(Collection<Deployer> deployers) {
    this.deployers = Collections.unmodifiableList(new ArrayList<>(deployers));
  }

  /** Returns a read-only snapshot of the deployers owned by this handle. */
  public synchronized Collection<Deployer> deployers() {
    if (closed) {
      throw new IllegalStateException("Deployer scope is closed");
    }
    return deployers;
  }

  /** Attempts every close and logs failures without changing the operation's outcome. */
  @Override
  public void close() {
    List<Deployer> owned;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      owned = deployers;
      deployers = Collections.emptyList();
    }
    Set<Deployer> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    RuntimeException failure = null;
    for (int i = owned.size() - 1; i >= 0; i--) {
      Deployer deployer = owned.get(i);
      if (!seen.add(deployer)) {
        continue;
      }
      try {
        deployer.close();
      } catch (RuntimeException closeFailure) {
        if (failure == null) {
          failure = closeFailure;
        } else if (failure != closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
    }
    if (failure != null) {
      LOG.warn("Failed to close deployers", failure);
    }
  }
}
