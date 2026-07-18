package com.linkedin.hoptimator;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * Capability implemented by a source's Calcite {@code Schema} (the object a driver/adapter already
 * instantiates for a {@code Database}) to report how far a {@code TableTrigger}'s input has
 * advanced in <em>data time</em>. It is the richer sibling of {@code LogicalSchemaMarker}: rather
 * than a one-bit marker, it carries methods, so a single source-agnostic reconciler can drive
 * event-time triggers for any datastore without a per-source controller or a separate plugin
 * registry.
 *
 * <p><b>Why hang this off the schema.</b> The schema was built from <em>this</em> {@code Database}'s
 * connection config (JDBC URL, operands, ...), so per-input configuration — e.g. which Kafka cluster
 * to read, addressed however that source addresses clusters — is inherent. There is no global
 * config to reach for and no ambiguity when many clusters are each their own {@code Database}. The
 * reconciler resolves a trigger's {@code (catalog, schema)} to the {@code Database}'s schema and, if
 * it implements this capability, asks it about the specific {@code table}.
 *
 * <p><b>Contract.</b> {@link #frontier(String)} returns an instant in <em>data time</em> — the
 * logical time of the records, not when a change happened to be observed — that is the latest point
 * for which input has <em>arrived</em>: the source has seen data through it. The reconciler advances
 * a trigger's cursor to this frontier and fires the job over the newly-available window. The value
 * must be <em>monotonic non-decreasing</em> for a given table across calls.
 *
 * <p><b>Frontier, not a completeness watermark — and repair is what licenses optimism.</b> The
 * frontier is an <em>optimistic</em> signal: it says "data has appeared through here," not
 * "everything at or before here has definitely arrived." A source may report an optimistic
 * frontier <em>only</em> if it can heal what lands <em>behind</em> the cursor: late or
 * out-of-order writes must be reported by {@link #changesSince(String, Instant)} and replayed as
 * one-off backfills, so completeness is achieved by <em>frontier + repair</em>. A source that does
 * <b>not</b> implement {@link #changesSince(String, Instant)} has no safety net and therefore
 * <b>must</b> report a <em>conservative</em> frontier — a true completeness watermark — or it will
 * silently drop late data. (The bundled Kafka {@code ClusterSchema} takes the conservative route: a
 * bounded out-of-orderness watermark — the per-partition min of the latest timestamp, minus a lag,
 * excluding idle partitions — so it is sound without any repair.)
 *
 * <p>Return {@link Optional#empty()} when a frontier cannot be determined right now; the reconciler
 * then falls back to cron/manual firing. A schema that does not implement this interface at all is
 * simply not frontier-driven.
 *
 * <p>Discovered by {@code HoptimatorJdbcSchema.inputFrontierSource()}, which unwraps the driver's
 * inner schema to this interface — no SPI file or shared string required, exactly like
 * {@code LogicalSchemaMarker}.
 */
public interface InputFrontierSource {

  /**
   * Returns the latest data-time instant through which {@code table} has received input, or
   * {@link Optional#empty()} if a frontier cannot be determined at this time. Monotonic
   * non-decreasing per table.
   */
  Optional<Instant> frontier(String table);

  /**
   * Returns changes to {@code table} observed strictly after {@code sinceArrival}, in arrival order,
   * each mapped to the data-time window it affects. The reconciler uses these to repair late or
   * out-of-order writes that land <em>behind</em> the cursor: a change whose window is already
   * behind the cursor is replayed as a one-off backfill over that window, leaving the forward cursor
   * untouched. Changes whose window is still ahead of the cursor are handled by normal forward
   * processing and need no repair.
   *
   * <p>The default returns an empty list: a source with no late-arrival semantics is purely
   * forward-frontier, and {@link #frontier(String)} alone drives it. {@code sinceArrival} may be
   * null, meaning "from the beginning of what the source is willing to report".
   */
  default List<DataChange> changesSince(String table, Instant sinceArrival) {
    return Collections.emptyList();
  }
}
