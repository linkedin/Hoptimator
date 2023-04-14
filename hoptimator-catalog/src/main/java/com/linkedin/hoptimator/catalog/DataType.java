package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;

/** Common data types. Not authoratitive or exhaustive. */
public enum DataType {

  VARCHAR_NULL(x -> x.createTypeWithNullability(x.createSqlType(SqlTypeName.VARCHAR), true)),
  VARCHAR_NOT_NULL(x -> x.createTypeWithNullability(x.createSqlType(SqlTypeName.VARCHAR), false));

  public static final RelDataTypeFactory DEFAULT_TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private final RelProtoDataType protoType;

  DataType(RelProtoDataType protoType) {
    this.protoType = protoType; 
  }

  public RelProtoDataType proto() {
    return protoType;
  }

  public RelDataType rel(RelDataTypeFactory typeFactory) {
    return protoType.apply(typeFactory);
  }

  public RelDataType rel() {
    return protoType.apply(DEFAULT_TYPE_FACTORY);
  }

  public static RelDataTypeFactory.Builder builder() {
    return new RelDataTypeFactory.Builder(DEFAULT_TYPE_FACTORY);
  }

  /** Convenience builder for non-scalar types */
  public static Struct struct() {
    return x -> x.createStructType(Collections.emptyList());
  }

  /** Convenience builder for non-scalar types */
  public interface Struct extends RelProtoDataType {
    default Struct with(String name, DataType dataType) {
      return x -> {
        RelDataType existing = apply(x);
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(x);
        builder.addAll(existing.getFieldList());
        builder.add(name, dataType.rel(x));
        return builder.build();
      };
    }

    default RelDataType rel() {
      return apply(DEFAULT_TYPE_FACTORY);
    }
  }
}
 
