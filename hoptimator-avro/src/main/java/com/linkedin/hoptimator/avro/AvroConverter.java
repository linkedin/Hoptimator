package com.linkedin.hoptimator.avro;

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

/** Converts between Avro and Calcite's RelDataType */
public final class AvroConverter {

  private AvroConverter() {
  }

  public static Schema avro(String namespace, String name, RelDataType dataType) {
    if (dataType.isStruct()) {
      List<Schema.Field> fields = dataType.getFieldList().stream()
        .map(x -> new Schema.Field(sanitize(x.getName()), avro(namespace, x.getName(), x.getType()), describe(x), null))
        .collect(Collectors.toList());
      return createAvroSchemaWithNullability(Schema.createRecord(sanitize(name), dataType.toString(), namespace, false, fields),
        dataType.isNullable());
    } else {
      switch (dataType.getSqlTypeName()) {
      case INTEGER:
        return createAvroTypeWithNullability(Schema.Type.INT, dataType.isNullable());
      case SMALLINT:
        return createAvroTypeWithNullability(Schema.Type.INT, dataType.isNullable());
      case BIGINT:
        return createAvroTypeWithNullability(Schema.Type.LONG, dataType.isNullable());
      case VARCHAR:
        return createAvroTypeWithNullability(Schema.Type.STRING, dataType.isNullable());
      case FLOAT:
        return createAvroTypeWithNullability(Schema.Type.FLOAT, dataType.isNullable());
      case DOUBLE:
        return createAvroTypeWithNullability(Schema.Type.DOUBLE, dataType.isNullable());
      case CHAR:
        return createAvroTypeWithNullability(Schema.Type.STRING, dataType.isNullable());
      case BOOLEAN:
        return createAvroTypeWithNullability(Schema.Type.BOOLEAN, dataType.isNullable());
      case ARRAY:
        return createAvroSchemaWithNullability(Schema.createArray(avro(null, null, dataType.getComponentType())),
          dataType.isNullable());
  // TODO support map types
  // Appears to require a Calcite version bump
  //    case MAP:
  //      return createAvroSchemaWithNullability(Schema.createMap(avroPrimitive(dataType.getValueType())), dataType.isNullable());
      case UNKNOWN:
      case NULL:
        return Schema.createUnion(Schema.create(Schema.Type.NULL));
      default:
        throw new UnsupportedOperationException("No support yet for " + dataType.getSqlTypeName().toString());
      }
    }
  }

  public static Schema avro(String namespace, String name, RelProtoDataType relProtoDataType) {
    RelDataTypeFactory factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return avro(namespace, name, relProtoDataType.apply(factory));
  }

  private static Schema createAvroSchemaWithNullability(Schema schema, boolean nullable) {
    if (nullable) {
      return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    } else {
      return schema;
    }
  }

  private static Schema createAvroTypeWithNullability(Schema.Type rawType, boolean nullable) {
    return createAvroSchemaWithNullability(Schema.create(rawType), nullable);
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
      return createRelType(typeFactory, SqlTypeName.INTEGER);
    case LONG:
      return createRelType(typeFactory, SqlTypeName.BIGINT);
    case ENUM:
    case FIXED:
    case STRING:
      return createRelType(typeFactory, SqlTypeName.VARCHAR);
    case FLOAT:
      return createRelType(typeFactory, SqlTypeName.FLOAT);
    case DOUBLE:
      return createRelType(typeFactory, SqlTypeName.DOUBLE);
    case BOOLEAN:
      return createRelType(typeFactory, SqlTypeName.BOOLEAN);
    case ARRAY:
      return typeFactory.createArrayType(rel(schema.getElementType(), typeFactory), -1);
//  TODO support map types
//  Appears to require a Calcite version bump
//    case MAP:
//      return typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR), rel(schema.getValueType(), typeFactory));
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
    return rel(schema, new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
  }

  private static RelDataType createRelType(RelDataTypeFactory typeFactory, SqlTypeName typeName) {
    RelDataType rawType = typeFactory.createSqlType(typeName);
    return typeFactory.createTypeWithNullability(rawType, false);
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
