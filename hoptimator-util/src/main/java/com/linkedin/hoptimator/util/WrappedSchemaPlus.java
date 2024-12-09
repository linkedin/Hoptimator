package com.linkedin.hoptimator.util;

import org.apache.calcite.schema.SchemaPlus;

import java.sql.Wrapper;

public class WrappedSchemaPlus implements Wrapper {

  private final SchemaPlus schemaPlus;

  public WrappedSchemaPlus(SchemaPlus schemaPlus) {
    this.schemaPlus = schemaPlus;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) {
    return (T) schemaPlus;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isAssignableFrom(SchemaPlus.class);
  }
}