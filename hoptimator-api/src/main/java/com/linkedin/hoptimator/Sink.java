package com.linkedin.hoptimator;

import java.util.List;
import java.util.Map;


// Unclear if Sink will always extend Source
public class Sink extends Source implements Deployable {

  public Sink(String database, List<String> path, Map<String, String> options) {
    super(database, path, options);
  }

  @Override
  public String toString() {
    return "Sink[" + pathString() + "]";
  }
}
