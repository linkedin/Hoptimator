package com.linkedin.hoptimator.util;

import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Map;

public class Sink extends Source {

  public Sink(String database, List<String> path, RelDataType rowType,
      Map<String, String> options) {
    super(database, path, rowType, options);
  }

  @Override
  public String toString() {
    return "Sink[" + pathString() + "]";
  }
}
