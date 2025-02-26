package com.linkedin.hoptimator;

import java.util.List;
import java.util.Map;


// Unclear if Sink will always extend Source
public class Sink extends Source implements Deployable {

  public Sink(String database, List<String> path, String partialViewName, Map<String, String> options) {
    super(database, path, partialViewName, options);
  }

  @Override
  public String toString() {
    return "Sink[" + pathString() + "]";
  }
}
