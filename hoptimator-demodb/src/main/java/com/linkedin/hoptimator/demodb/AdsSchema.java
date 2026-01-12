package com.linkedin.hoptimator.demodb;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;

import com.linkedin.hoptimator.jdbc.schema.LazyTableLookup;


public class AdsSchema extends AbstractSchema {

  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public AdsSchema() {
  }

  @Override
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyTableLookup<>() {

      @Override
      protected Map<String, Table> loadAllTables() {
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put("PAGE_VIEWS", new PageViewTable());
        tableMap.put("AD_CLICKS", new AdClickTable());
        tableMap.put("CAMPAIGNS", new CampaignTable());
        return tableMap;
      }

      @Override
      protected @Nullable Table loadTable(String name) {
        switch (name) {
          case "PAGE_VIEWS":
            return new PageViewTable();
          case "AD_CLICKS":
            return new AdClickTable();
          case "CAMPAIGNS":
            return new CampaignTable();
          default:
            return null;
        }
      }

      @Override
      protected String getSchemaDescription() {
        return "Demo Ads Schema";
      }
    });
  }
}
