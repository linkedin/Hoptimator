package com.linkedin.hoptimator;

import java.util.List;
import java.util.function.Function;


public class MaterializedView {

  private final String database;
  private final List<String> path;
  private final String viewSql;
  private final Function<SqlDialect, String> pipelineSql;
  private final Pipeline pipeline;

  public MaterializedView(String database, List<String> path, String viewSql, Function<SqlDialect, String> pipelineSql,
      Pipeline pipeline) {
    this.database = database;
    this.path = path;
    this.viewSql = viewSql;
    this.pipelineSql = pipelineSql;
    this.pipeline = pipeline;
  }

  /** SQL query which defines this view, e.g. SELECT ... FROM ... */
  public String viewSql() {
    return viewSql;
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

  public String table() {
    return path.get(path.size() - 1);
  }

  public String schema() {
    return path.get(path.size() - 2);
  }

  public List<String> path() {
    return path;
  }

  protected String pathString() {
    return String.join(".", path);
  }

  @Override
  public String toString() {
    return "MaterializedView[" + pathString() + "]";
  }
}
