package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public enum DataType {

  VARCHAR(x -> x.createSqlType(SqlTypeName.VARCHAR)),
  VARCHAR_NOT_NULL(x -> x.createTypeWithNullability(x.createSqlType(SqlTypeName.VARCHAR), false));

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

  private final RelProtoDataType protoType;

  public static final RelDataTypeFactory DEFAULT_TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
}
  
