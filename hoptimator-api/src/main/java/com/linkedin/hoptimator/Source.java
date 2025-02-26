package com.linkedin.hoptimator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class Source implements Deployable {

  private final String database;
  private final List<String> path;
  private final String partialViewName;
  private final Map<String, String> options;

  public Source(String database, List<String> path, String partialViewName, Map<String, String> options) {
    this.database = database;
    this.path = path;
    this.partialViewName = partialViewName;
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

  public String schema() {
    return path.get(path.size() - 2);
  }

  public List<String> path() {
    return path;
  }

  public String partialViewName() {
    return partialViewName;
  }

  protected String pathString() {
    return path.stream().collect(Collectors.joining("."));
  }

  @Override
  public String toString() {
    return "Source[" + pathString() + "]";
  }
}
