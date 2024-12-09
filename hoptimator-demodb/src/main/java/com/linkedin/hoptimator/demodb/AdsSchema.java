package com.linkedin.hoptimator.demodb;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;


public class AdsSchema extends AbstractSchema {

  private final Map<String, Table> tableMap = new HashMap<>();

  public AdsSchema() {
    tableMap.put("PAGE_VIEWS", new PageViewTable());
    tableMap.put("AD_CLICKS", new AdClickTable());
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
