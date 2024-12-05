package com.linkedin.hoptimator.jdbc.schema;

import com.linkedin.hoptimator.Catalog;
import com.linkedin.hoptimator.jdbc.CatalogService;
import com.linkedin.hoptimator.util.RemoteTable;

import org.apache.calcite.schema.Schema;

/** A table populated with all available Catlaogs. */
public class CatalogTable extends RemoteTable<Catalog, CatalogTable.Row> {

  // This and other Row classes are used by generated code, so it is important
  // that they follow this pattern.
  // CHECKSTYLE:OFF
  public static class Row {
    public String NAME;
    public String DESCRIPTION;

    public Row(String name, String description) {
      this.NAME = name;
      this.DESCRIPTION = description;
    }
  }
  // CHECKSTYLE:ON

  public CatalogTable() {
    super(CatalogService.API, Row.class);
  }

  @Override
  public Row toRow(Catalog catalog) {
    return new Row(catalog.name(), catalog.description());
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SYSTEM_TABLE;
  }
}
