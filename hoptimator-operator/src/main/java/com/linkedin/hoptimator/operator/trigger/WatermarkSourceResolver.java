package com.linkedin.hoptimator.operator.trigger;

import com.linkedin.hoptimator.InputWatermarkSource;


/**
 * Resolves a trigger's input {@code (catalog, schema)} to the {@link InputWatermarkSource} capability
 * of that {@code Database}'s schema, or {@code null} when the input is not watermark-driven (no such
 * database, or its driver surfaces no watermark capability). The production implementation unwraps
 * the operator's Calcite connection via {@code DeployerUtils.jdbcSchema(...).inputWatermarkSource()};
 * tests supply a fake. This is the seam that replaces the old per-source {@code InputWatermarkProvider}
 * SPI: the capability now hangs off the object the driver already builds.
 */
@FunctionalInterface
public interface WatermarkSourceResolver {

  /** Returns the input's watermark source, or {@code null} if the input is not watermark-driven. */
  InputWatermarkSource resolve(String catalog, String schema);
}
