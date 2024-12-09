package com.linkedin.hoptimator.util;

import java.util.function.Function;

import org.apache.calcite.sql.SqlDialect;


public class Job {

  private final Sink sink;
  private final Function<SqlDialect, String> sql;

  public Job(Sink sink, Function<SqlDialect, String> sql) {
    this.sink = sink;
    this.sql = sql;
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
