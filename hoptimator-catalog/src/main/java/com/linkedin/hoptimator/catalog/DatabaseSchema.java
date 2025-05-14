package com.linkedin.hoptimator.catalog;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;


/** Exposes a Database to Apache Calcite. */
public class DatabaseSchema extends AbstractSchema {
  private final Database database;
  private final Map<String, Table> tableMap;

  public DatabaseSchema(Database database, Map<String, Table> tableMap) {
    this.database = database;
    this.tableMap = tableMap;
  }

  public static DatabaseSchema create(Database database) {
    try {
      Map<String, Table> tableMap = database.tables().stream().collect(Collectors.toMap(x -> x, x -> new ProtoTable(x, database)));
      return new DatabaseSchema(database, tableMap);
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
