package com.linkedin.hoptimator;

import java.util.List;


public class View implements Deployable {

  private final String database;
  private final List<String> path;
  private final String viewSql;

  public View(String database, List<String> path, String viewSql) {
    this.database = database;
    this.path = path;
    this.viewSql = viewSql;
  }

  /** The internal name for the database this view belongs to, or null if unknown. */
  public String database() {
    return database;
  }

  /** SQL query which defines this view, e.g. SELECT ... FROM ... */
  public String viewSql() {
    return viewSql;
  }

  public String table() {
    return path.get(path.size() - 1);
  }

  /**
   * Returns the schema name if present.
   */
  public String schema() {
    return path.size() >= 2 ? path.get(path.size() - 2) : null;
  }

  /**
   * Returns the catalog name if present (3-level path), or null for 2-level paths.
   */
  public String catalog() {
    return path.size() >= 3 ? path.get(path.size() - 3) : null;
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
