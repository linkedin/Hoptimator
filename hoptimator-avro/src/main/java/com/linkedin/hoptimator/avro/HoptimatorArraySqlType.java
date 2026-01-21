package com.linkedin.hoptimator.avro;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.ArraySqlType;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;


// Custom ArraySqlType to fix digest compatibility issue with Calcite JdbcSchema.
// JdbcSchema expects row types digest format to match something like "INTEGER ARRAY". JdbcSchema is incapable of parsing
// the array type if the digest format is like "INTEGER NOT NULL ARRAY" or "INTEGER ARRAY NOT NULL".
// Nullability for the inner element is not supported by Calcite at all but nullability for the array itself is supported
// and obtained via the isNullable() method instead.
// Ideally we should be doing this for all SqlTypes but Calcite JdbcSchema is currently only checking ArraySqlType digest format.
public class HoptimatorArraySqlType extends ArraySqlType {

  public HoptimatorArraySqlType(RelDataType elementType, boolean isNullable) {
    super(elementType, isNullable);
  }

  @Override
  @SuppressWarnings("method.invocation.invalid")
  protected void computeDigest(@UnknownInitialization HoptimatorArraySqlType this) {
    StringBuilder sb = new StringBuilder();
    generateTypeString(sb, true);
    digest = sb.toString();
  }
}
