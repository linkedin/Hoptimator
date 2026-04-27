package com.linkedin.hoptimator;

import java.util.List;


public class MaterializedView extends View implements Deployable {

  private final ThrowingFunction<SqlDialect, String> pipelineSql;
  private final Pipeline pipeline;

  public MaterializedView(String database, List<String> path, String viewSql, ThrowingFunction<SqlDialect, String> pipelineSql,
      Pipeline pipeline) {
    super(database, path, viewSql);
    this.pipelineSql = pipelineSql;
    this.pipeline = pipeline;
  }

  public Pipeline pipeline() {
    return pipeline;
  }

  public ThrowingFunction<SqlDialect, String> pipelineSql() {
    return pipelineSql;
  }

  @Override
  public String toString() {
    return "MaterializedView[" + pathString() + "]";
  }
}
