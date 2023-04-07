package com.linkedin.hoptimator.catalog;

import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class AdapterImpl implements Adapter {
  private final String database;
  private final TableLister tableLister;
  private final TableResolver tableResolver;
  private final TableFactory tableFactory;

  public AdapterImpl(String database, TableLister tableLister, TableResolver tableResolver, TableFactory tableFactory) {
    this.database = database;
    this.tableLister = tableLister;
    this.tableResolver = tableResolver;
    this.tableFactory = tableFactory;
  }

  /** Convenience constructor for simple connector-based tables */
  public AdapterImpl(String database, TableLister tableLister, TableResolver tableResolver, ConfigProvider configProvider) {
    this(database, tableLister, tableResolver, TableFactory.connector(configProvider));
  }

  /** Convenience constructor for a list of connector-based tables */
  public AdapterImpl(String database, Collection<String> tables, TableResolver tableResolver, ConfigProvider configProvider) {
    this(database, tables, tableResolver, TableFactory.connector(configProvider));
  }

  /** Convenience constructor for a static list of tables */
  public AdapterImpl(String database, Collection<String> tables, TableResolver tableResolver, TableFactory tableFactory) {
    this(database, () -> tables, tableResolver, tableFactory);
  }

  /** Convenience constructor for a static table map */
  public AdapterImpl(String database, Map<String, AdapterTable> tableMap) {
    this(database, () -> Collections.unmodifiableCollection(tableMap.keySet()),
      x -> tableMap.get(x).rowType(), (x, y, z) -> tableMap.get(y));
  }

  @Override
  public String database() {
    return database;
  }

  @Override
  public AdapterTable table(String name) throws InterruptedException, ExecutionException {
    return tableFactory.table(database(), name, tableResolver.resolve(name));
  }

  @Override
  public Collection<String> list() throws InterruptedException, ExecutionException {
    return tableLister.list();
  }
}
