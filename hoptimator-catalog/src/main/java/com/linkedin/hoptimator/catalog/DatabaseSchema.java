package com.linkedin.hoptimator.catalog;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;


/** Exposes a Database to Apache Calcite. */
public class DatabaseSchema extends AbstractSchema {
  private final Database database;
  private final Map<String, Table> tableMap;

  public DatabaseSchema(Database database) {
    this.database = database;
    try {
      this.tableMap = database.tables().stream().collect(Collectors.toMap(x -> x, x -> new ProtoTable(x, database)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Database database() {
    return database;
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
