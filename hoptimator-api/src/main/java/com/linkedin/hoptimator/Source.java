package com.linkedin.hoptimator;

import java.util.List;
import java.util.Map;

public class Source implements Deployable {

  private final String database;
  private final List<String> path;
  private final Map<String, String> options;

  public Source(String database, List<String> path, Map<String, String> options) {
    this.database = database;
    this.path = path;
    this.options = options;
  }

  public Map<String, String> options() {
    return options;
  }

  /** The internal name for the database this table belongs to. Not necessary the same as schema. */
  public String database() {
    return database;
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

  public String pathString() {
    return String.join(".", path);
  }

  @Override
  public String toString() {
    return "Source[" + pathString() + "]";
  }
}
