package com.linkedin.hoptimator;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * Capability implemented by a source's Calcite {@code Schema} (the object a driver/adapter already
 * instantiates for a {@code Database}) to report how far a {@code TableTrigger}'s input is complete
 * in <em>data time</em>. It is the richer sibling of {@code LogicalSchemaMarker}: rather than a
 * one-bit marker, it carries methods, so a single source-agnostic reconciler can drive event-time
 * triggers for any datastore without a per-source controller or a separate plugin registry.
 *
 * <p><b>Why hang this off the schema.</b> The schema was built from <em>this</em> {@code Database}'s
 * connection config (JDBC URL, operands, ...), so per-input configuration — e.g. which Kafka cluster
 * to read, addressed however that source addresses clusters — is inherent. There is no global
 * config to reach for and no ambiguity when many clusters are each their own {@code Database}. The
 * reconciler resolves a trigger's {@code (catalog, schema)} to the {@code Database}'s schema and, if
 * it implements this capability, asks it about the specific {@code table}.
 *
 * <p><b>Contract.</b> {@link #watermark(String)} returns an instant in <em>data time</em> —
 * the logical time of the records, not when a change happened to be observed — through which the
 * input is known to be <em>complete</em>: no record with an earlier data-time will still arrive. The
 * value must be <em>monotonic non-decreasing</em> for a given table across calls. The reconciler
 * advances a trigger's cursor to this watermark and never past it, so honoring data-time +
 * monotonicity is what makes the cursor sound.
 *
 * <p>Return {@link Optional#empty()} when a watermark cannot be determined right now; the reconciler
 * then falls back to cron/manual firing. A schema that does not implement this interface at all is
 * simply not watermark-driven.
 *
 * <p>Inputs whose data arrives genuinely out of order — where no monotonic data-time watermark
 * exists — should hold {@link #watermark(String)} back to the latest gap-free point and report
 * the out-of-order writes via {@link #changesSince(String, Instant)} instead.
 *
 * <p>Discovered by {@code HoptimatorJdbcSchema.inputWatermarkSource()}, which unwraps the driver's
 * inner schema to this interface — no SPI file or shared string required, exactly like
 * {@code LogicalSchemaMarker}.
 */
public interface InputWatermarkSource {

  /**
   * Returns the data-time instant through which {@code table} is known to be complete, or
   * {@link Optional#empty()} if a watermark cannot be determined at this time.
   */
  Optional<Instant> watermark(String table);

  /**
   * Returns changes to {@code table} observed strictly after {@code sinceArrival}, in arrival order,
   * each mapped to the data-time window it affects. The reconciler uses these to repair late or
   * out-of-order writes that land <em>behind</em> the watermark: a change whose window is already
   * behind the cursor is replayed as a one-off backfill over that window, leaving the forward cursor
   * untouched. Changes whose window is still ahead of the cursor are handled by normal forward
   * processing and need no repair.
   *
   * <p>The default returns an empty list: a source with no late-arrival semantics is purely
   * forward-frontier, and {@link #watermark(String)} alone drives it. {@code sinceArrival} may
   * be null, meaning "from the beginning of what the source is willing to report".
   */
  default List<DataChange> changesSince(String table, Instant sinceArrival) {
    return Collections.emptyList();
  }
}
