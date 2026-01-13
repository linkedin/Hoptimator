package com.linkedin.hoptimator.demodb;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;

import com.linkedin.hoptimator.jdbc.schema.LazyTableLookup;


public class ProfileSchema extends AbstractSchema {

  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public ProfileSchema() {
  }

  @Override
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyTableLookup<>() {

      @Override
      protected Map<String, Table> loadAllTables() {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("MEMBERS", new MemberTable());
        tableMap.put("COMPANIES", new CompanyTable());
        return tableMap;
      }

      @Override
      protected @Nullable Table loadTable(String name) {
        switch (name) {
          case "MEMBERS":
            return new MemberTable();
          case "COMPANIES":
            return new CompanyTable();
          default:
            return null;
        }
      }

      @Override
      protected String getSchemaDescription() {
        return "Demo Profile Schema";
      }
    });
  }
}
