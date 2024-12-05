package com.linkedin.hoptimator.jdbc.schema;

import com.linkedin.hoptimator.util.Api;
import com.linkedin.hoptimator.util.RemoteTable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collection;
import java.util.Collections;

/** A table prints whatever you INSERT into it. */
public class PrintTable extends RemoteTable<String, Object> {

  private static final Api<String> API = new Api<String>() {

    @Override
    public Collection<String> list() {
      return Collections.emptyList();
    }

    @Override
    public void create(String s) {
      System.out.println(s);
    }
  };

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder().add("OUTPUT", SqlTypeName.VARCHAR, 0).build();
  }

  public PrintTable() {
    super(API, Object.class);
  }

  @Override
  public Object toRow(String s) {
    return s;
  }

  @Override
  public String fromRow(Object row) {
    return row.toString();
  }
}
