package com.linkedin.hoptimator.catalog;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;
import java.util.stream.Collectors;

/** Exposes an Adapter's namespace to Apache Calcite */
public class AdapterSchema extends AbstractSchema {
  private final Map<String, Table> tableMap;

  public AdapterSchema(String database, Adapter adapter) {
    try {
      this.tableMap = adapter.list().stream()
        .collect(Collectors.toMap(x -> x, x -> new ProtoTable(x, adapter)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
