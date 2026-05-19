package com.linkedin.hoptimator.util.planner;

/**
 * Marker interface implemented by Calcite {@code Schema} instances that surface Hoptimator
 * LogicalTable-style CRDs — i.e. schemas whose tables aren't physical resources but logical
 * declarations bound to one or more tiers.
 *
 * <p><b>Why this exists.</b> {@link HoptimatorJdbcSchema} is the outer JDBC adapter that
 * fronts every {@code Database} CRD in the user's connection. When the underlying driver
 * surfaces logical tables (e.g. {@code jdbc:logical://...}), the outer adapter needs a way
 * to tell — without baking driver-specific URL prefixes or class names into its own logic.
 *
 * <p>The inner schema implements this marker, and {@link HoptimatorJdbcSchema#isLogical()}
 * lazily opens its downstream connection on first ask, walks its root sub-schemas, and
 * surfaces {@code true} when any of them unwraps to this marker. Multiple logical-table
 * drivers can participate by having their inner schemas implement the same interface;
 * no SPI or shared string is required.
 *
 * <p>Intentionally empty. Promote to a real API only if a future caller needs more than the
 * one-bit "is this schema logical?" signal.
 */
public interface LogicalSchemaMarker {
}
