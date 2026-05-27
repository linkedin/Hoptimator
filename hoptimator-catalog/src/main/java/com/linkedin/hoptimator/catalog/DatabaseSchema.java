package com.linkedin.hoptimator.catalog;

import com.linkedin.hoptimator.jdbc.schema.LazyLookup;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.stream.Collectors;


/** Exposes a Database to Apache Calcite. */
public class DatabaseSchema extends AbstractSchema {
  private final Database database;
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public DatabaseSchema(Database database) {
    this.database = database;
  }

  public Database database() {
    return database;
  }

  @Override
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyLookup<>() {

      @Override
      protected Map<String, Table> loadAll() throws Exception {
        return database.tables().stream().collect(Collectors.toMap(x -> x, x -> new ProtoTable(x, database)));
      }

      @Override
      protected @Nullable Table load(String name) throws Exception {
        if (database.tables().contains(name)) {
          return new ProtoTable(name, database);
        }
        return null;
      }

      @Override
      protected String getDescription() {
        return "Database: " + database.toString();
      }
    });
  }
}
