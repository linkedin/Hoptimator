package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.concurrent.ExecutionException;

/** Resolves a table name into a concrete row type. Usually involves a network call. */
public interface TableResolver {
  RelDataType resolve(String table) throws InterruptedException, ExecutionException;

  /** Appends an extra column to the resolved type */
  default TableResolver with(String name, RelDataType dataType) {
    return x -> {
      RelDataType rowType = resolve(x);
      RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(DataType.DEFAULT_TYPE_FACTORY);
      builder.addAll(rowType.getFieldList());
      builder.add(name, dataType);
      return builder.build();
    };
  }
}
