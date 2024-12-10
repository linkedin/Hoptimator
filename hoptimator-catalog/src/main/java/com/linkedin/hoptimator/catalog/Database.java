package com.linkedin.hoptimator.catalog;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.calcite.rel.type.RelDataType;


/** A set of tables with unique names */
public class Database {
  private final String name;
  private final TableLister tableLister;
  private final TableResolver tableResolver;
  private final TableFactory tableFactory;

  public Database(String name, TableLister tableLister, TableResolver tableResolver, TableFactory tableFactory) {
    this.name = name;
    this.tableLister = tableLister;
    this.tableResolver = tableResolver;
    this.tableFactory = tableFactory;
  }

  /** Convenience constructor for simple connector-based tables */
  public Database(String name, TableLister lister, TableResolver resolver, ConfigProvider configs) {
    this(name, lister, resolver, TableFactory.connector(configs));
  }

  /** Convenience constructor for simple connector-based tables with resources */
  public Database(String name, TableLister lister, TableResolver resolver, ConfigProvider configs,
      ResourceProvider resources) {
    this(name, lister, resolver, TableFactory.connector(configs, resources));
  }

  /** Convenience constructor for a list of connector-based tables */
  public Database(String name, Collection<String> tables, TableResolver resolver, ConfigProvider configs) {
    this(name, tables, resolver, TableFactory.connector(configs));
  }

  /** Convenience constructor for a static list of tables */
  public Database(String name, Collection<String> tables, TableResolver resolver, TableFactory tableFactory) {
    this(name, () -> tables, resolver, tableFactory);
  }

  /** Convenience constructor for a static table map */
  public Database(String name, Map<String, HopTable> tableMap) {
    this(name, () -> Collections.unmodifiableCollection(tableMap.keySet()), x -> tableMap.get(x).rowType(),
        (x, y, z) -> tableMap.get(y));
  }

  /** Find a specific table in the database. */
  public HopTable table(String tableName) throws InterruptedException, ExecutionException {
    return tableFactory.table(this.name, tableName, tableResolver.resolve(tableName));
  }

  /** List tables in the database. */
  public Collection<String> tables() {
    return tableLister.list();
  }

  /** Construct a new table within this database. */
  public HopTable makeTable(String tableName, RelDataType rowType) {
    return tableFactory.table(this.name, tableName, rowType);
  }
}
