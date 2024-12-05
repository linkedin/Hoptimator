package com.linkedin.hoptimator.util;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Map;

public class MaterializedView extends Sink {

  private final CalcitePrepare.Context context;
  private final String sql;

  public MaterializedView(CalcitePrepare.Context context, String database, List<String> path,
      RelDataType rowType, String sql, Map<String, String> options) {
    super(database, path, rowType, options);
    this.context = context;
    this.sql = sql;
  }

  /** Context required to evaluate the view */
  public CalcitePrepare.Context context() {
    return context;
  }

  public String sql() {
    return sql;
  }

  @Override
  public String toString() {
    return "MaterializedView[" + pathString() + "]";
  }
}
