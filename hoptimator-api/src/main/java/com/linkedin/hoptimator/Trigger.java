package com.linkedin.hoptimator;

import java.util.Map;


public class Trigger implements Deployable {

  public static final String PAUSED_OPTION = "paused";

  private final String name;
  private final UserJob job;
  private final String cronSchedule;
  private final Map<String, String> options;
  private final Source source;
  private final Sink sink;

  public Trigger(String name, UserJob job, String cronSchedule, Map<String, String> options,
      Source source) {
    this(name, job, cronSchedule, options, source, null);
  }

  /**
   * Variant accepting an optional downstream sink. Set by {@code LogicalTableDeployer} on
   * bridging-tier triggers so the visualizer can render the directional flow
   * {@code source -.-> trigger -.-> sink}, and so the dep-guard finds the sink-side
   * dependency. {@code null} for user-created triggers (no declared sink).
   */
  public Trigger(String name, UserJob job, String cronSchedule, Map<String, String> options,
      Source source, Sink sink) {
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
  public Sink sink() {
    return sink;
  }

  @Override
  public String toString() {
    String path = source == null ? "<unbound>" : String.join(".", source.path());
    return "Trigger[" + name + ", " + path + "]";
  }
}
