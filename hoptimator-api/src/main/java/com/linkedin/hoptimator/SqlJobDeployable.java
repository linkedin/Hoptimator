package com.linkedin.hoptimator;

import java.util.List;
import java.util.Map;


/** Represents a CREATE JOB request for deploying a SqlJob to Kubernetes. */
public class SqlJobDeployable implements Deployable {

  private final String name;
  private final String dialect;
  private final String executionMode;
  private final List<String> sql;
  private final Map<String, String> options;

  public SqlJobDeployable(String name, String dialect, String executionMode,
      List<String> sql, Map<String, String> options) {
    this.name = name;
    this.dialect = dialect;
    this.executionMode = executionMode;
    this.sql = sql;
    this.options = options;
  }

  public String name() {
    return name;
  }

  /** Dialect, e.g. "Flink", "FlinkBeam". May be null for default. */
  public String dialect() {
    return dialect;
  }

  /** Execution mode, e.g. "Streaming", "Batch". May be null for default. */
  public String executionMode() {
    return executionMode;
  }

  public List<String> sql() {
    return sql;
  }

  public Map<String, String> options() {
    return options;
  }

  @Override
  public String toString() {
    return "SqlJob[" + name + "]";
  }
}
