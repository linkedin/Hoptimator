package com.linkedin.hoptimator;

import java.util.List;


public class View implements Deployable {

  private final List<String> path;
  private final String viewSql;

  public View(List<String> path, String viewSql) {
    this.path = path;
    this.viewSql = viewSql;
  }

  /** SQL query which defines this view, e.g. SELECT ... FROM ... */
  public String viewSql() {
    return viewSql;
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
    return "View[" + pathString() + "]";
  }
}
