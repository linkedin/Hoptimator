package com.linkedin.hoptimator.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.type.SqlTypeName;


/**
 * A permissive opaque function used for Calcite schema registration.
 *
 * <p>When users register UDFs via CREATE FUNCTION, this class provides a
 * placeholder so that queries referencing the UDF pass Calcite validation.
 * The actual function implementation is provided by the runtime (e.g., Flink).
 *
 * <p>Each overload accepts a fixed number of ANY-typed parameters and returns
 * a configurable type (defaulting to VARCHAR). ANY parameters allow any column
 * type to be passed without casting. The return type can be specified via the
 * RETURNS option in CREATE FUNCTION ... WITH (RETURNS 'INTEGER').
 *
 * <p>At execution time (e.g., in test query previews), the function returns null.
 */
public final class OpaqueFunction implements ScalarFunction, ImplementableFunction {

  private static final int MAX_ARITY = 10;
  private final int arity;
  private final SqlTypeName returnType;

  private OpaqueFunction(int arity, SqlTypeName returnType) {
    this.arity = arity;
    this.returnType = returnType;
  }

  /**
   * Creates a list of opaque function overloads covering arities 0 through {@link #MAX_ARITY}
   * with the given return type.
   */
  public static List<OpaqueFunction> overloads(SqlTypeName returnType) {
    return overloads(MAX_ARITY, returnType);
  }

  /**
   * Creates a list of opaque function overloads covering arities 0 through maxArity.
   */
  public static List<OpaqueFunction> overloads(int maxArity, SqlTypeName returnType) {
    List<OpaqueFunction> functions = new ArrayList<>();
    for (int i = 0; i <= maxArity; i++) {
      functions.add(new OpaqueFunction(i, returnType));
    }
    return Collections.unmodifiableList(functions);
  }

  @Override
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(returnType), true);
  }

  @Override
  public List<FunctionParameter> getParameters() {
    List<FunctionParameter> params = new ArrayList<>();
    for (int i = 0; i < arity; i++) {
      final int ordinal = i;
      params.add(new FunctionParameter() {
        @Override
        public int getOrdinal() {
          return ordinal;
        }

        @Override
        public String getName() {
          return "param" + ordinal;
        }

        @Override
        public RelDataType getType(RelDataTypeFactory typeFactory) {
          return typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.ANY), true);
        }

        @Override
        public boolean isOptional() {
          return false;
        }
      });
    }
    return params;
  }

  @Override
  public CallImplementor getImplementor() {
    // Returns null for any invocation. UDFs are opaque placeholders in the
    // JDBC driver; they only truly execute in the data plane (e.g., Flink).
    return (translator, call, nullAs) -> Expressions.constant(null, javaType(returnType));
  }

  private static Class<?> javaType(SqlTypeName typeName) {
    switch (typeName) {
      case BOOLEAN:
        return Boolean.class;
      case TINYINT:
        return Byte.class;
      case SMALLINT:
        return Short.class;
      case INTEGER:
        return Integer.class;
      case BIGINT:
        return Long.class;
      case FLOAT:
      case REAL:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case DECIMAL:
        return java.math.BigDecimal.class;
      default:
        return String.class;
    }
  }
}
