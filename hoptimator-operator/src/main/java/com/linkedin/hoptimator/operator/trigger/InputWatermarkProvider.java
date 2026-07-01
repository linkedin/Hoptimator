package com.linkedin.hoptimator.operator.trigger;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * Service Provider Interface that reports how far a {@code TableTrigger}'s input is complete, so a
 * single, source-agnostic reconciler can drive event-time triggers for any datastore. A source
 * integration implements this SPI instead of contributing its own trigger controller.
 *
 * <p><b>Contract.</b> {@link #completeThrough(TriggerInput)} returns an instant in <em>data
 * time</em> — the logical time of the records, not when a change happened to be observed or
 * published — through which the input is known to be <em>complete</em>: no record with an earlier
 * data-time will still arrive. The value must be <em>monotonic non-decreasing</em> for a given
 * input across calls (a completeness watermark only moves forward). The reconciler advances a
 * trigger's cursor to this watermark and never past it, so honoring data-time + monotonicity is what
 * makes the cursor sound.
 *
 * <p>Return {@link Optional#empty()} when this provider does not handle the input (e.g. a different
 * catalog/datastore) or cannot determine a watermark right now; the reconciler then consults the
 * next provider, falling back to cron/manual firing when none applies.
 *
 * <p>Inputs whose data arrives genuinely out of order — where no monotonic data-time watermark
 * exists — are not a fit for this SPI; they belong to the partition-granular processing path
 * instead. Providers for such sources should expose a conservative watermark (holding back to the
 * latest gap-free point) or decline by returning empty.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}; register them with a
 * {@code META-INF/services/com.linkedin.hoptimator.operator.trigger.InputWatermarkProvider} file.
 */
public interface InputWatermarkProvider {

  /**
   * Returns the data-time instant through which {@code input} is known to be complete, or
   * {@link Optional#empty()} if this provider does not handle the input or cannot determine a
   * watermark at this time.
   */
  Optional<Instant> completeThrough(TriggerInput input);

  /**
   * Returns changes to {@code input} observed strictly after {@code sinceArrival}, in arrival order,
   * each mapped to the data-time window it affects. The reconciler uses these to repair late or
   * out-of-order writes that land <em>behind</em> the watermark: a change whose window is already
   * behind the cursor is replayed as a one-off backfill over that window, leaving the forward cursor
   * untouched. Changes whose window is still ahead of the cursor are handled by normal forward
   * processing and need no repair.
   *
   * <p>The default returns an empty list: a source with no late-arrival semantics is purely
   * forward-frontier, and {@link #completeThrough(TriggerInput)} alone drives it. {@code sinceArrival}
   * may be null, meaning "from the beginning of what the provider is willing to report".
   */
  default List<DataChange> changesSince(TriggerInput input, Instant sinceArrival) {
    return Collections.emptyList();
  }
}
