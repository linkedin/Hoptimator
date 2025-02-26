package com.linkedin.hoptimator.jdbc.schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.linkedin.hoptimator.Catalog;


/** Built-in utility tables. */
public class UtilityCatalog extends AbstractSchema implements Catalog {

  private final Map<String, Table> tableMap = new LinkedHashMap<>();

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
  public void register(Wrapper wrapper) throws SQLException {
    wrapper.unwrap(SchemaPlus.class).add("UTIL", this);
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }
}
