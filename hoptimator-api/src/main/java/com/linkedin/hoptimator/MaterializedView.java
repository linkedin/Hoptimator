package com.linkedin.hoptimator;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


public class MaterializedView extends View implements Deployable {

  private final String database;
  private final Function<SqlDialect, String> pipelineSql;
  private final Pipeline pipeline;

  public MaterializedView(String database, List<String> path, String viewSql, Function<SqlDialect, String> pipelineSql,
      Pipeline pipeline) {
    super(path, viewSql);
    this.database = database;
    this.pipelineSql = pipelineSql;
    this.pipeline = pipeline;
  }

  public Pipeline pipeline() {
    return pipeline;
  }

  public Function<SqlDialect, String> pipelineSql() {
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
