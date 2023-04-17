package com.linkedin.hoptimator.planner;

import com.linkedin.hoptimator.catalog.Resource;

/** Anything that can run SQL, e.g. a Flink job */
public class SqlJob extends Resource {
  public SqlJob(String sql) {
    super("SqlJob");
    export("sql", sql);
  }
}
