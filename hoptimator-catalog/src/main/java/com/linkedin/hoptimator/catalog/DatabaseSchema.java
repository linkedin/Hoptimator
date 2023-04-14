package com.linkedin.hoptimator.catalog;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;
import java.util.stream.Collectors;

/** Exposes a Database to Apache Calcite. */
public class DatabaseSchema extends AbstractSchema {
  private final Map<String, Table> tableMap;

  public DatabaseSchema(Database database) {
    try {
      this.tableMap = database.tables().stream()
        .collect(Collectors.toMap(x -> x, x -> new ProtoTable(x, database)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
