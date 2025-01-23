package com.linkedin.hoptimator;

import java.util.function.Function;


public class Job {

  private final String name;
  private final Sink sink;
  private final Function<SqlDialect, String> sql;

  public Job(String name, Sink sink, Function<SqlDialect, String> sql) {
    this.name = name;
    this.sink = sink;
    this.sql = sql;
  }

  public String name() {
    return name;
  }

  public Sink sink() {
    return sink;
  }

  public Function<SqlDialect, String> sql() {
    return sql;
  }

  @Override
  public String toString() {
    return "Job[" + sink.pathString() + "]";
  }
}
