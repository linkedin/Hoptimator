package com.linkedin.hoptimator.jdbc.schema;

import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.linkedin.hoptimator.Catalog;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;


/** Built-in utility tables. */
public class UtilityCatalog extends AbstractSchema implements Catalog {

  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public UtilityCatalog() {
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
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyTableLookup<>() {

      @Override
      protected Map<String, Table> loadAllTables() {
        return Collections.singletonMap("PRINT", new PrintTable());
      }

      @Override
      protected @Nullable Table loadTable(String name) {
        if ("PRINT".equals(name)) {
          return new PrintTable();
        }
        return null;
      }

      @Override
      protected String getSchemaDescription() {
        return "Utility Catalog";
      }
    });
  }
}
