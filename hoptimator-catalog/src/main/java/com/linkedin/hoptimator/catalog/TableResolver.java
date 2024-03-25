package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/** Resolves a table name into a concrete row type. Usually involves a network call. */
public interface TableResolver {
  RelDataType resolve(String table) throws InterruptedException, ExecutionException;

  static TableResolver from(Function<String, RelDataType> f) {
    return x -> f.apply(x);
  }

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

  default TableResolver with(String name, DataType dataType) {
    return with(name, dataType.rel());
  }

  default TableResolver with(String name, DataType.Struct struct) {
    return with(name, struct.rel());
  }

  default TableResolver mapStruct(Function<DataType.Struct, DataType.Struct> f) {
    return x -> f.apply(DataType.struct(resolve(x))).rel();
  }

  default TableResolver map(Function<RelDataType, RelDataType> f) {
    return x -> f.apply(resolve(x));
  }
}
