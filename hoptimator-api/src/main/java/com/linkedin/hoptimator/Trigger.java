package com.linkedin.hoptimator;

import javax.annotation.Nullable;
import java.util.Map;


public class Trigger implements Deployable {

  public static final String PAUSED_OPTION = "paused";

  private final String name;
  private final UserJob job;
  private final String cronSchedule;
  private final Map<String, String> options;
  private final Source source;
  private final Sink sink;

  /**
   * Contains an optional downstream sink for triggers that operate between a source
   * sink (think ETL/rETL).
   * TODO: need to collapse the "job.properties.online.table.name" logic into a sink for adhoc triggers
   */
  public Trigger(String name, UserJob job, String cronSchedule, Map<String, String> options,
      Source source, @Nullable Sink sink) {
    this.name = name;
    this.job = job;
    this.cronSchedule = cronSchedule;
    this.options = options;
    this.source = source;
    this.sink = sink;
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

  @Override
  public String toString() {
    String path = source == null ? "<unbound>" : String.join(".", source.path());
    return "Trigger[" + name + ", " + path + "]";
  }
}
