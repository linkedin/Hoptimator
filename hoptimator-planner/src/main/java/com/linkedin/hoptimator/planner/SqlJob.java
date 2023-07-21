package com.linkedin.hoptimator.planner;

import com.linkedin.hoptimator.catalog.Resource;

/**
 * Anything that can run SQL, e.g. a Flink job.
 *
 * The planner generates these, but they are not directly deployable. Instead,
 * an adapter should provide a template that turns SqlJobs into something
 * concrete and deployable, e.g. a FlinkDeployment.
 *
 * To do so, an adapter just needs to include a proper `SqlJob.template.yaml`.
 *
 */
public class SqlJob extends Resource {

  public SqlJob(String sql) {
    super("SqlJob");
    export("pipeline.sql", sql);
  }
}
