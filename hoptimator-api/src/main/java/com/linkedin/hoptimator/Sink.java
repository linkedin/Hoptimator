package com.linkedin.hoptimator;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;


// Unclear if Sink will always extend Source
public class Sink extends Source implements Deployable {

  public Sink(String database, List<String> path, Map<String, String> options, @Nullable RelDataType rowType) {
    super(database, path, options, rowType);
  }

  @Override
  public String toString() {
    return "Sink[" + pathString() + "]";
  }
}
