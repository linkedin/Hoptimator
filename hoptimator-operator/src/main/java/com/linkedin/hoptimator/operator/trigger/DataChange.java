package com.linkedin.hoptimator.operator.trigger;

import java.time.Instant;
import java.util.Objects;


/**
 * A change to an input observed at {@link #arrival()} that affects the data-time window
 * {@code [windowStart, windowEnd)}. Reported by an {@link InputWatermarkProvider} so the reconciler
 * can repair late or out-of-order writes that land <em>behind</em> the watermark — by running a
 * one-off backfill over the affected window — without disturbing the monotone forward cursor.
 *
 * <p>{@link #arrival()} is the time the change was observed (e.g. a commit/publish time); it is the
 * monotone axis the reconciler consumes the change stream on. {@code windowStart}/{@code windowEnd}
 * are in <em>data time</em> — the logical time range the changed data belongs to (e.g. the bounds of
 * a changed partition).
 */
public final class DataChange {
  private final Instant arrival;
  private final Instant windowStart;
  private final Instant windowEnd;

  public DataChange(Instant arrival, Instant windowStart, Instant windowEnd) {
    this.arrival = Objects.requireNonNull(arrival, "arrival");
    this.windowStart = Objects.requireNonNull(windowStart, "windowStart");
    this.windowEnd = Objects.requireNonNull(windowEnd, "windowEnd");
  }

  /** When the change was observed; the monotone axis used to consume the change stream in order. */
  public Instant arrival() {
    return arrival;
  }

  /** Inclusive data-time start of the affected window. */
  public Instant windowStart() {
    return windowStart;
  }

  /** Exclusive data-time end of the affected window. */
  public Instant windowEnd() {
    return windowEnd;
  }

  @Override
  public String toString() {
    return "DataChange{arrival=" + arrival + ", window=[" + windowStart + ", " + windowEnd + ")}";
  }
}
