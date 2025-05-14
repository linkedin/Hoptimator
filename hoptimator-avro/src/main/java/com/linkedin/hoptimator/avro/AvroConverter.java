package com.linkedin.hoptimator.avro;

import java.util.AbstractMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

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
        case SMALLINT:
          return createAvroTypeWithNullability(Schema.Type.INT, dataType.isNullable());
        case BIGINT:
          return createAvroTypeWithNullability(Schema.Type.LONG, dataType.isNullable());
        case VARCHAR:
        case CHAR:
          return createAvroTypeWithNullability(Schema.Type.STRING, dataType.isNullable());
        case FLOAT:
          return createAvroTypeWithNullability(Schema.Type.FLOAT, dataType.isNullable());
        case DOUBLE:
          return createAvroTypeWithNullability(Schema.Type.DOUBLE, dataType.isNullable());
        case BOOLEAN:
          return createAvroTypeWithNullability(Schema.Type.BOOLEAN, dataType.isNullable());
        case ARRAY:
          return createAvroSchemaWithNullability(Schema.createArray(avro(null, null, Objects.requireNonNull(dataType.getComponentType()))),
              dataType.isNullable());
        case MAP:
          return createAvroSchemaWithNullability(Schema.createMap(avro(null, null, Objects.requireNonNull(dataType.getValueType()))),
              dataType.isNullable());
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
    return rel(schema, typeFactory, false);
  }

  /** Converts Avro Schema to RelDataType.
   * Nullability is preserved except for array types, JDBC is incapable of interpreting e.g. "FLOAT NOT NULL ARRAY"
   * causing "NOT NULL" arrays to get demoted to "ANY ARRAY" which is not desired.
   */
  public static RelDataType rel(Schema schema, RelDataTypeFactory typeFactory, boolean nullable) {
    RelDataType unknown = typeFactory.createUnknownType();
    switch (schema.getType()) {
      case RECORD:
        return typeFactory.createTypeWithNullability(typeFactory.createStructType(schema.getFields().stream()
            .map(x -> new AbstractMap.SimpleEntry<>(x.name(), rel(x.schema(), typeFactory, nullable)))
            .filter(x -> x.getValue().getSqlTypeName() != SqlTypeName.NULL)
            .filter(x -> x.getValue().getSqlTypeName() != unknown.getSqlTypeName())
            .collect(Collectors.toList())), nullable);
      case INT:
        return createRelType(typeFactory, SqlTypeName.INTEGER, nullable);
      case LONG:
        return createRelType(typeFactory, SqlTypeName.BIGINT, nullable);
      case ENUM:
      case STRING:
        return createRelType(typeFactory, SqlTypeName.VARCHAR, nullable);
      case FIXED:
        return createRelType(typeFactory, SqlTypeName.VARBINARY, schema.getFixedSize(), nullable);
      case BYTES:
        return createRelType(typeFactory, SqlTypeName.VARBINARY, nullable);
      case FLOAT:
        return createRelType(typeFactory, SqlTypeName.FLOAT, nullable);
      case DOUBLE:
        return createRelType(typeFactory, SqlTypeName.DOUBLE, nullable);
      case BOOLEAN:
        return createRelType(typeFactory, SqlTypeName.BOOLEAN, nullable);
      case ARRAY:
        return typeFactory.createTypeWithNullability(
            typeFactory.createArrayType(rel(schema.getElementType(), typeFactory, true), -1), nullable);
      case MAP:
        return typeFactory.createTypeWithNullability(
            typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR), rel(schema.getValueType(), typeFactory, nullable)), nullable);
      case UNION:
        boolean isNullable = schema.isNullable();
        if (schema.isNullable() && schema.getTypes().size() == 2) {
          Optional<Schema> innerType = schema.getTypes().stream().filter(x -> x.getType() != Schema.Type.NULL).findFirst();
          if (innerType.isEmpty()) {
            throw new IllegalArgumentException("Union schema must contain at least one non-null type");
          }
          return typeFactory.createTypeWithNullability(rel(innerType.get(), typeFactory, true), true);
        }
        // Since we collapse complex unions into separate fields, each of these fields needs to be nullable
        // as only one of the group will be present in any given record.
        return typeFactory.createTypeWithNullability(typeFactory.createStructType(schema.getTypes().stream()
            .filter(x -> x.getType() != Schema.Type.NULL)
            .map(x -> new AbstractMap.SimpleEntry<>(x.getName(), rel(x, typeFactory, true)))
            .filter(x -> x.getValue().getSqlTypeName() != SqlTypeName.NULL)
            .filter(x -> x.getValue().getSqlTypeName() != unknown.getSqlTypeName())
            .collect(Collectors.toList())), isNullable);
      default:
        return typeFactory.createTypeWithNullability(typeFactory.createUnknownType(), true);
    }
  }

  public static RelDataType rel(Schema schema) {
    return rel(schema, new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
  }

  private static RelDataType createRelType(RelDataTypeFactory typeFactory, SqlTypeName typeName, boolean nullable) {
    return createRelType(typeFactory, typeName, RelDataType.PRECISION_NOT_SPECIFIED, nullable);
  }

  private static RelDataType createRelType(RelDataTypeFactory typeFactory, SqlTypeName typeName,
      int precision, boolean nullable) {
    RelDataType rawType = typeFactory.createSqlType(typeName, precision);
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
