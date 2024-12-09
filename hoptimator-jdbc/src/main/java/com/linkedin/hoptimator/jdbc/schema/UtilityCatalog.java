package com.linkedin.hoptimator.jdbc.schema;

import com.linkedin.hoptimator.Catalog;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;
import java.util.HashMap;
import java.sql.Wrapper;
import java.sql.SQLException;

/** Built-in utility tables. */
public class UtilityCatalog extends AbstractSchema implements Catalog {

  private final Map<String, Table> tableMap = new HashMap<>();

  public UtilityCatalog() {
    tableMap.put("PRINT", new PrintTable());
  }

  @Override
  public String name() {
    return "util";
  }

  @Override
  public String description() {
    return "Built-in utility tables";
  }

  @Override
  public void register(Wrapper parentSchema) throws SQLException {
    parentSchema.unwrap(SchemaPlus.class).add("UTIL", this);
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }
}