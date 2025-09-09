package com.linkedin.hoptimator;

import java.util.List;


public class MaterializedView extends View implements Deployable {

  private final String database;
  private final ThrowingFunction<SqlDialect, String> pipelineSql;
  private final Pipeline pipeline;

  public MaterializedView(String database, List<String> path, String viewSql, ThrowingFunction<SqlDialect, String> pipelineSql,
      Pipeline pipeline) {
    super(path, viewSql);
    this.database = database;
    this.pipelineSql = pipelineSql;
    this.pipeline = pipeline;
  }

  public Pipeline pipeline() {
    return pipeline;
  }

  public ThrowingFunction<SqlDialect, String> pipelineSql() {
    return pipelineSql;
  }

  /** The internal name for the database this table belongs to. Not necessary the same as schema. */
  public String database() {
    return database;
  }

  @Override
  public String toString() {
    return "MaterializedView[" + pathString() + "]";
  }
}
