package com.linkedin.hoptimator.demodb;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public class ProfileSchema extends AbstractSchema {

  private final Map<String, Table> tableMap = new HashMap<>();

  public ProfileSchema() {
    tableMap.put("MEMBERS", new MemberTable());
    tableMap.put("COMPANIES", new CompanyTable());
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}