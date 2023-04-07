package com.linkedin.hoptimator.catalog;

import org.apache.avro.Schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.AbstractMap;
import java.util.List;
import java.util.stream.Collectors;

public final class AvroConverter {

  private AvroConverter() {
  }

  public static Schema avro(String name, String namespace, RelDataType dataType) {
    if (dataType.isStruct()) {
      List<Schema.Field> fields = dataType.getFieldList().stream()
        .filter(x -> !x.getName().startsWith("__")) // don't write out hidden fields
        .map(x -> new Schema.Field(sanitize(x.getName()), avro(x.getName(), namespace, x.getType()), describe(x), null))
        .collect(Collectors.toList());
      return Schema.createRecord(sanitize(name), dataType.toString(), namespace, false, fields);
    } else {
      switch (dataType.getSqlTypeName()) {
      case INTEGER:
        return createAvroTypeWithNullability(Schema.Type.INT, dataType.isNullable());
      case VARCHAR:
        return createAvroTypeWithNullability(Schema.Type.STRING, dataType.isNullable());
      case FLOAT:
        return createAvroTypeWithNullability(Schema.Type.FLOAT, dataType.isNullable());
      case DOUBLE:
        return createAvroTypeWithNullability(Schema.Type.DOUBLE, dataType.isNullable());
      case CHAR:
        return createAvroTypeWithNullability(Schema.Type.STRING, dataType.isNullable());
      case UNKNOWN:
      case NULL:
        return Schema.createUnion(Schema.create(Schema.Type.NULL));
      default:
        throw new UnsupportedOperationException("No support yet for " + dataType.getSqlTypeName().toString());
      }
    }
  }

  public static Schema avro(String name, String namespace, RelProtoDataType relProtoDataType) {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return avro(name, namespace, relProtoDataType.apply(factory));
  }

  private static Schema createAvroTypeWithNullability(Schema.Type rawType, boolean nullable) {
    if (nullable) {
      return Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(rawType)); 
    } else {
      return Schema.create(rawType);
    }
  }

  public static RelDataType rel(Schema schema, RelDataTypeFactory typeFactory) {
    RelDataType unknown = typeFactory.createUnknownType();
    switch (schema.getType()) {
    case RECORD:
      return typeFactory.createStructType(schema.getFields().stream()
        .map(x -> new AbstractMap.SimpleEntry<>(x.name(), rel(x.schema(), typeFactory)))
        .filter(x -> x.getValue().getSqlTypeName() != SqlTypeName.NULL)
        .filter(x -> x.getValue().getSqlTypeName() != unknown.getSqlTypeName())
        .collect(Collectors.toList()));
    case INT:
    case LONG:
      // schema.isNullable() should be false for basic types iiuc
      return createRelTypeWithNullability(typeFactory, SqlTypeName.INTEGER, schema.isNullable());
    case ENUM:
    case STRING:
      return createRelTypeWithNullability(typeFactory, SqlTypeName.VARCHAR, schema.isNullable());
    case FLOAT:
      return createRelTypeWithNullability(typeFactory, SqlTypeName.FLOAT, schema.isNullable());
    case DOUBLE:
      return createRelTypeWithNullability(typeFactory, SqlTypeName.DOUBLE, schema.isNullable());
    case UNION:
      if (schema.isNullable() && schema.getTypes().size() == 2) {
        Schema innerType = schema.getTypes().stream().filter(x -> x.getType() != Schema.Type.NULL).findFirst().get();
        return typeFactory.createTypeWithNullability(rel(innerType, typeFactory), true);
      } else {
        // TODO support more elaborate union types
        return typeFactory.createTypeWithNullability(typeFactory.createUnknownType(), true);
      }
    default:
      return typeFactory.createUnknownType();
    }
  }

  public static RelDataType rel(Schema schema) {
    return rel(schema, DataType.DEFAULT_TYPE_FACTORY);
  }

  private static RelDataType createRelTypeWithNullability(RelDataTypeFactory typeFactory, SqlTypeName typeName, boolean nullable) {
    RelDataType rawType = typeFactory.createSqlType(typeName);
    return typeFactory.createTypeWithNullability(rawType, nullable);
  }

  public static RelProtoDataType proto(Schema schema) {
    return RelDataTypeImpl.proto(rel(schema, new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)));
  }

  private static String describe(RelDataTypeField dataType) {
    return dataType.getName() + " " + dataType.getType().getFullTypeString();
  }

  private static String sanitize(String name) {
    if (name.matches("^[^A-Za-z_]")) {
      // avoid starting with numbers, etc
      return sanitize("_" + name);
    }
    // avoid $, etc
    return name.replaceAll("\\W", "");
  }
}
