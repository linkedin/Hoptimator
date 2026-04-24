package com.linkedin.hoptimator;

import java.sql.SQLException;
import java.util.Collection;


/**
 * Implemented by deployers that manage resources (Kafka topics, Venice stores, logical tables, …)
 * which other pipelines may reference. The framework reads {@link #guardedResources()} before
 * {@link Deployer#delete()} and, via a {@link DependencyChecker} loaded from the classpath,
 * blocks deletion while active pipelines still depend on the resource.
 *
 * <p>Deployers here are purely declarative — they say <em>what</em> to guard, not how. The
 * actual check is performed by {@code DeploymentService.delete} using a {@link DependencyChecker}
 * SPI implementation (registered by e.g. {@code hoptimator-k8s}). This keeps backend-specific
 * concerns (Kubernetes API access, label selectors, etc.) out of the individual deployer modules
 * — {@code hoptimator-kafka} and friends never import {@code hoptimator-k8s}.
 *
 * <p>Mirrors the {@link Validated} / {@code ValidationService} layering.
 */
public interface DependencyGuarded {

  /**
   * The resources this deployer manages that may be referenced by external pipelines. Returned
   * as a collection so composite deployers (e.g. a logical table spanning multiple tiers) can
   * declare all of them in a single place.
   */
  Collection<Source> guardedResources() throws SQLException;

  /**
   * UID of an owner CRD whose <em>owned</em> pipelines are considered "self" and exempted from
   * the external-dependents check, because they will be cascade-deleted alongside the resource.
   * Return {@code null} (the default) when there is no such self-owner — any dependent pipeline
   * blocks deletion.
   */
  default String selfOwnerUid() throws SQLException {
    return null;
  }
}
