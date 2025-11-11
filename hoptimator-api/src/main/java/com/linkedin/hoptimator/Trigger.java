package com.linkedin.hoptimator;

import java.util.List;
import java.util.Map;


public class Trigger implements Deployable {

  private final String name;
  private final UserJob job;
  private final List<String> path;
  private final String cronSchedule;
  private final Map<String, String> options;

  public Trigger(String name, UserJob job, List<String> path, String cronSchedule,
      Map<String, String> options) {
    this.name = name;
    this.job = job;
    this.path = path;
    this.cronSchedule = cronSchedule;
    this.options = options;
  }

  public String name() {
    return name;
  }

  public List<String> path() {
    return path;
  }

  public UserJob job() {
    return job;
  }

  public String cronSchedule() {
    return cronSchedule;
  }

  public String table() {
    return path.get(path.size() - 1);
  }

  /**
   * Returns the schema name if present.
   */
  public String schema() {
    return path.size() >= 2 ? path.get(path.size() - 2) : null;
  }

  public Map<String, String> options() {
    return options;
  }

  private String pathString() {
    return String.join(".", path);
  }

  @Override
  public String toString() {
    return "Trigger[" + name() + ", " + pathString() + "]";
  }
}
