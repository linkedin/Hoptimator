package com.linkedin.hoptimator;

import java.util.Map;


/** Represents a CREATE DATABASE request. */
public class DatabaseDeployable implements Deployable {

  private final String name;
  private final Map<String, String> options;

  public DatabaseDeployable(String name, Map<String, String> options) {
    this.name = name;
    this.options = options;
  }

  public String name() {
    return name;
  }

  public Map<String, String> options() {
    return options;
  }

  @Override
  public String toString() {
    return "Database[" + name + "]";
  }
}
