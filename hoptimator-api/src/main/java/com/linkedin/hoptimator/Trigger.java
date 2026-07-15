package com.linkedin.hoptimator;

import javax.annotation.Nullable;
import java.time.OffsetDateTime;
import java.util.Map;


public class Trigger implements Deployable {

  public static final String PAUSED_OPTION = "paused";

  /**
   * A {@code FIRE TRIGGER} request. A plain fire (both bounds null) bumps the cursor; a windowed
   * fire ({@link #from()}/{@link #to()} both set) requests a one-off backfill over {@code [from,
   * to]} without moving the cursor. Bounds are already resolved to absolute UTC instants.
   */
  public static final class Fire {
    private final OffsetDateTime from;
    private final OffsetDateTime to;

    public Fire(@Nullable OffsetDateTime from, @Nullable OffsetDateTime to) {
      this.from = from;
      this.to = to;
    }

    /** Backfill window start, or null for a plain fire. */
    public @Nullable OffsetDateTime from() {
      return from;
    }

    /** Backfill window end, or null for a plain fire. */
    public @Nullable OffsetDateTime to() {
      return to;
    }

    /** True when this is a windowed fire (a one-off backfill), false for a plain fire. */
    public boolean windowed() {
      return from != null && to != null;
    }
  }

  private final String name;
  private final UserJob job;
  private final String cronSchedule;
  private final Map<String, String> options;
  private final Source source;
  private final Sink sink;
  private final Fire fire;

  /**
   * Contains an optional downstream sink for triggers that operate between a source
   * sink (think ETL/rETL).
   * TODO: need to collapse the "job.properties.online.table.name" logic into a sink for adhoc triggers
   */
  public Trigger(String name, UserJob job, String cronSchedule, Map<String, String> options,
      Source source, @Nullable Sink sink) {
    this(name, job, cronSchedule, options, source, sink, null);
  }

  public Trigger(String name, UserJob job, String cronSchedule, Map<String, String> options,
      Source source, @Nullable Sink sink, @Nullable Fire fire) {
    this.name = name;
    this.job = job;
    this.cronSchedule = cronSchedule;
    this.options = options;
    this.source = source;
    this.sink = sink;
    this.fire = fire;
  }

  public String name() {
    return name;
  }

  public UserJob job() {
    return job;
  }

  public String cronSchedule() {
    return cronSchedule;
  }

  public Map<String, String> options() {
    return options;
  }

  /** Upstream source the trigger fires on, or {@code null} when only the name is known
   *  (e.g. during DROP TRIGGER / PAUSE / RESUME, which only need to look up the existing CRD). */
  public Source source() {
    return source;
  }

  /** Downstream sink the trigger's job writes to, or {@code null} when the trigger has no declared sink. */
  public @Nullable Sink sink() {
    return sink;
  }

  /** The {@code FIRE TRIGGER} request this deploy carries, or {@code null} for any non-fire operation
   *  (CREATE / DROP / PAUSE / RESUME). */
  public @Nullable Fire fire() {
    return fire;
  }

  @Override
  public String toString() {
    String path = source == null ? "<unbound>" : String.join(".", source.path());
    return "Trigger[" + name + ", " + path + "]";
  }
}
