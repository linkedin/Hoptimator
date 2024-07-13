package com.linkedin.hoptimator.catalog;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Collections;
import java.util.stream.Collectors;

/** Common data types. Not authoratitive or exhaustive. */
public enum DataType {

  VARCHAR(x -> x.createTypeWithNullability(x.createSqlType(SqlTypeName.VARCHAR), true)),
  VARCHAR_NOT_NULL(x -> x.createTypeWithNullability(x.createSqlType(SqlTypeName.VARCHAR), false)),
  NULL(x -> x.createSqlType(SqlTypeName.NULL));

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
    if (protoType == null) {
      return null;
    } else {
      return protoType.apply(DEFAULT_TYPE_FACTORY);
    } 
  }

  public static RelDataTypeFactory.Builder builder() {
    return new RelDataTypeFactory.Builder(DEFAULT_TYPE_FACTORY);
  }

  /** Convenience builder for non-scalar types */
  public static Struct struct() {
    return x -> x.createStructType(Collections.emptyList());
  }

  public static Struct struct(RelDataType relDataType) {
    return x -> relDataType;
  }

  /** Convenience builder for non-scalar types */
  public interface Struct extends RelProtoDataType {

    default Struct with(String name, RelDataType dataType) {
      return x -> {
        RelDataType existing = apply(x);
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(x);
        builder.addAll(existing.getFieldList());
        builder.add(name, dataType);
        return builder.build();
      };
    }

    default Struct with(String name, DataType dataType) {
      return with(name, dataType.rel());
    }

    default Struct with(String name, Struct struct) {
      return with(name, struct.rel());
    }

    default RelDataType rel() {
      return apply(DEFAULT_TYPE_FACTORY);
    }

    default String sql() {
      return (new ScriptImplementor.RowTypeSpecImplementor(rel())).sql();
    }

    default Struct drop(String name) {
      return x -> {
        RelDataType dataType = apply(x);
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(x);
        builder.addAll(dataType.getFieldList().stream()
          .filter(y -> !y.getName().equals(name))
          .collect(Collectors.toList()));
        return builder.build();
      };
    }

    default Struct dropNestedRows() {
      return x -> {
        RelDataType dataType = apply(x);
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(x);
        builder.addAll(dataType.getFieldList().stream()
          .filter(y -> y.getType().getSqlTypeName() != SqlTypeName.ROW)
          .collect(Collectors.toList()));
        return builder.build();
      };
    }

    default Struct get(String name) {
      return x -> {
        RelDataTypeField field = apply(x).getField(name, true, false);
        if (field == null) {
          return null;
        } else {
          RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(x);
          builder.add(field);
          return builder.build();
        }
      };
    } 
  }
}
 
