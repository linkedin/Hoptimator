package com.linkedin.hoptimator.jdbc;

import java.sql.Wrapper;

import org.apache.calcite.schema.SchemaPlus;


/** Wraps internal impls to prevent leaking them to the API. */
public class Wrapped implements Wrapper {

  private final HoptimatorConnection connection;
  private final SchemaPlus schemaPlus;

  public Wrapped(HoptimatorConnection connection, SchemaPlus schemaPlus) {
    this.connection = connection;
    this.schemaPlus = schemaPlus;
  }

  public HoptimatorConnection connection() {
    return connection;
  }

  public SchemaPlus schema() {
    return schemaPlus;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) {
    if (iface.isAssignableFrom(HoptimatorConnection.class)) {
      return (T) connection;
    } else if (iface.isAssignableFrom(SchemaPlus.class)) {
      return (T) schemaPlus;
    } else {
      return null;
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isAssignableFrom(SchemaPlus.class)
        || iface.isAssignableFrom(HoptimatorConnection.class);
  }
}
