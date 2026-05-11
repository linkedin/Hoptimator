package com.linkedin.hoptimator;

import java.sql.Connection;
import java.sql.SQLException;


/**
 * SPI for backends that can materialize a {@link PipelineGraph} for a given {@link GraphTarget}.
 * Discovered at runtime via {@code ServiceLoader} — implementations register themselves through
 * a {@code META-INF/services/com.linkedin.hoptimator.GraphProvider} file in their module.
 *
 * <p>Mirrors the {@link DeployerProvider} shape: {@code supports(target)} narrows what each
 * provider handles, and {@link #priority()} orders multiple providers when more than one
 * supports the same target.
 */
public interface GraphProvider {

  /**
   * Build a graph for the given target. Should only be called when {@link #supports(GraphTarget)}
   * returns {@code true} for the same target.
   */
  PipelineGraph forTarget(GraphTarget target, int depth, Connection connection) throws SQLException;

  /** Whether this provider can handle the given target kind. */
  boolean supports(GraphTarget target);
}
