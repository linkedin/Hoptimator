package com.linkedin.hoptimator.avro;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
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
import org.apache.calcite.util.Pair;


/** Converts between Avro and Calcite's RelDataType */
public final class AvroConverter {

  private static final String KEY_OPTION = "key.fields";
  private static final String KEY_PREFIX_OPTION = "key.fields-prefix";
  private static final String PRIMITIVE_KEY = "KEY";

  private AvroConverter() {
  }

  public static Schema avro(String namespace, String name, RelDataType dataType) {
    // TODO: Schema generation is overly verbose today and does not support reuse, hence why we always defined a new namespace for records.
    // Ideally information should be extracted from the RelDataType to allow collapsing of records into one definition.
    String newNamespace = namespace + "." + name;
    if (dataType.isStruct()) {
      List<Schema.Field> fields = dataType.getFieldList().stream()
          .map(x -> {
            Schema innerField = avro(newNamespace, x.getName(), x.getType());
            Object defaultValue = null;
            // For unions containing null, defaults are specified in a specific way
            if (innerField.isUnion() && innerField.isNullable()) {
              defaultValue = Schema.Field.NULL_DEFAULT_VALUE;
            }
            return new Schema.Field(sanitize(x.getName()), innerField, describe(x), defaultValue);
          })
          .collect(Collectors.toList());
      return createAvroSchemaWithNullability(Schema.createRecord(sanitize(name), dataType.toString(), newNamespace, false, fields),
          dataType.isNullable());
    } else {
      switch (dataType.getSqlTypeName()) {
        case INTEGER:
        case SMALLINT:
        case TINYINT:
          return createAvroTypeWithNullability(Schema.Type.INT, dataType.isNullable());
        case BIGINT:
          return createAvroTypeWithNullability(Schema.Type.LONG, dataType.isNullable());
        case VARCHAR:
        case CHAR:
          return createAvroTypeWithNullability(Schema.Type.STRING, dataType.isNullable());
        case FLOAT:
          return createAvroTypeWithNullability(Schema.Type.FLOAT, dataType.isNullable());
        case BINARY:
        case VARBINARY:
          if (dataType.getPrecision() != -1) {
            return createAvroSchemaWithNullability(Schema.createFixed(sanitize(name), dataType.toString(), newNamespace,
                dataType.getPrecision()), dataType.isNullable());
          } else {
            return createAvroTypeWithNullability(Schema.Type.BYTES, dataType.isNullable());
          }
        case DOUBLE:
          return createAvroTypeWithNullability(Schema.Type.DOUBLE, dataType.isNullable());
        case BOOLEAN:
          return createAvroTypeWithNullability(Schema.Type.BOOLEAN, dataType.isNullable());
        case ARRAY:
          return createAvroSchemaWithNullability(
              Schema.createArray(avro(newNamespace, sanitize(name) + "ArrayElement",
                  Objects.requireNonNull(dataType.getComponentType()))),
              dataType.isNullable());
        case MAP:
          return createAvroSchemaWithNullability(
              Schema.createMap(avro(newNamespace, sanitize(name) + "MapElement",
                  Objects.requireNonNull(dataType.getValueType()))),
              dataType.isNullable());
        case UNKNOWN:
        case NULL:
          // We can't have a union of null and null ["null", "null"], nor a nested union ["null",["null"]],
          // so we ignore nullability and just use ["null"] here.
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

  // Users expect key options to create a key schema for the key fields and a payload schema for the rest.
  // A key schema will not always be present and will be returned as null if not.
  // Returns a pair of Pair<key schema, payload schema>
  public static Pair<Schema, Schema> avroKeyPayloadSchema(String namespace, String keySchemaName, String payloadSchemaName,
      RelDataType dataType, Map<String, String> keyOptions) {
    String keys = keyOptions.get(KEY_OPTION);
    String keyPrefix = keyOptions.getOrDefault(KEY_PREFIX_OPTION, "");

    // If no keys are provided or the RelDataType is a primitive, return just a payload schema
    if (keys == null || keys.isEmpty() || !dataType.isStruct()) {
      return new Pair<>(null, avro(namespace, payloadSchemaName, dataType));
    }

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder keyBuilder = new RelDataTypeFactory.Builder(typeFactory);
    RelDataTypeFactory.Builder payloadBuilder = new RelDataTypeFactory.Builder(typeFactory);

    List<String> keyNames = List.of(keys.split(";"));

    Schema primitiveKeySchema = null;
    for (RelDataTypeField field : dataType.getFieldList()) {
      if (keyNames.contains(field.getName())) {
        String keyName = field.getName().substring(keyPrefix.length());

        // Key is a primitive
        if (keyNames.size() == 1 && keyName.equals(PRIMITIVE_KEY)) {
          primitiveKeySchema = avro(namespace, keySchemaName, field.getType());
        } else {
          keyBuilder.add(keyName, field.getType());
        }
      } else {
        payloadBuilder.add(field);
      }
    }
    if (primitiveKeySchema != null) {
      return new Pair<>(primitiveKeySchema, avro(namespace, payloadSchemaName, payloadBuilder.build()));
    }
    return new Pair<>(avro(namespace, keySchemaName, keyBuilder.build()),
        avro(namespace, payloadSchemaName, payloadBuilder.build()));
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
   * causing "NOT NULL" arrays to get demoted to "ANY ARRAY" which is not desired. See HoptimatorArraySqlType for
   * more details.
   *
   * TODO: default field values are lost when converting from Avro to RelDataType
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
        return new HoptimatorArraySqlType(rel(schema.getElementType(), typeFactory, true), nullable);
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
