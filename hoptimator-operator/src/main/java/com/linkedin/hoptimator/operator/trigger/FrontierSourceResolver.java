package com.linkedin.hoptimator.operator.trigger;

import com.linkedin.hoptimator.InputFrontierSource;


/**
 * Resolves a trigger's input {@code (catalog, schema)} to the {@link InputFrontierSource} capability
 * of that {@code Database}'s schema, or {@code null} when the input is not frontier-driven (no such
 * database, or its driver surfaces no frontier capability). The production implementation unwraps
 * the operator's Calcite connection via {@code DeployerUtils.jdbcSchema(...).inputFrontierSource()};
 * tests supply a fake. This is the seam that replaces the old per-source {@code InputWatermarkProvider}
 * SPI: the capability now hangs off the object the driver already builds.
 */
@FunctionalInterface
public interface FrontierSourceResolver {

  /** Returns the input's frontier source, or {@code null} if the input is not frontier-driven. */
  InputFrontierSource resolve(String catalog, String schema);
}
