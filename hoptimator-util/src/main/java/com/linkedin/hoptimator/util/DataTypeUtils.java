package com.linkedin.hoptimator.util;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;


public final class DataTypeUtils {

  private static final String ARRAY_TYPE = "__ARRTYPE__";
  private static final String MAP_KEY_TYPE = "__MAPKEYTYPE__";
  private static final String MAP_VALUE_TYPE = "__MAPVALUETYPE__";

  private DataTypeUtils() {
  }

  /**
   * Flattens nested structs and complex arrays.
   * <p>
   * Nested structs like `FOO Row(BAR Row(QUX VARCHAR))` are promoted to
   * top-level fields like `FOO$BAR$QUX VARCHAR`.
   * <p>
   * Complex arrays like `FOO Row(BAR Row(QUX VARCHAR)) ARRAY` are promoted to
   * top-level fields like `FOO ANY ARRAY` and `FOO$BAR$QUX VARCHAR`.
   * Primitive arrays are unchanged.
   */
  public static RelDataType flatten(RelDataType dataType, RelDataTypeFactory typeFactory) {
    if (!dataType.isStruct()) {
      return dataType;
    }
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    flattenInto(typeFactory, dataType, builder, Collections.emptyList());
    return builder.build();
  }

  private static void flattenInto(RelDataTypeFactory typeFactory, RelDataType dataType,
      RelDataTypeFactory.Builder builder, List<String> path) {
    if (dataType.getComponentType() != null) {
      // Handles ARRAY types
      if (dataType.getComponentType().isStruct()) {
        // Handles arrays of record types
        builder.add(String.join("$", path), typeFactory.createArrayType(
            typeFactory.createSqlType(SqlTypeName.ANY), -1));
        for (RelDataTypeField field : dataType.getComponentType().getFieldList()) {
          flattenInto(typeFactory, field.getType(), builder, Stream.concat(path.stream(),
              Stream.of(field.getName())).collect(Collectors.toList()));
        }
      } else if (dataType.getComponentType() instanceof BasicSqlType) {
        // Handles primitive arrays
        builder.add(String.join("$", path), dataType);
      } else {
        // Handles nested arrays
        builder.add(String.join("$", path), typeFactory.createArrayType(
            typeFactory.createSqlType(SqlTypeName.ANY), -1));
        flattenInto(typeFactory, dataType.getComponentType(), builder, Stream.concat(path.stream(),
            Stream.of(ARRAY_TYPE)).collect(Collectors.toList()));
      }
    } else if (dataType.isStruct()) {
      // Handles Record types
      for (RelDataTypeField field : dataType.getFieldList()) {
        flattenInto(typeFactory, field.getType(), builder, Stream.concat(path.stream(),
            Stream.of(field.getName())).collect(Collectors.toList()));
      }
    } else if (dataType.getKeyType() != null && dataType.getValueType() != null) {
      // Handles map types
      builder.add(String.join("$", path) + "$" + MAP_KEY_TYPE, dataType.getKeyType());
      flattenInto(typeFactory, dataType.getValueType(), builder, Stream.concat(path.stream(),
          Stream.of(MAP_VALUE_TYPE)).collect(Collectors.toList()));
    } else {
      // Handles primitive types
      builder.add(String.join("$", path), dataType);
    }
  }

  /** Restructures flattened types, from `FOO$BAR VARCHAR` to `FOO Row(BAR VARCHAR...)`
   * The combination of fields `FOO ANY ARRAY` and `FOO$BAR$QUX VARCHAR` is reconstructed
   * into `FOO Row(BAR Row(QUX VARCHAR)) ARRAY`
   */
  public static RelDataType unflatten(RelDataType dataType, RelDataTypeFactory typeFactory) {
    if (!dataType.isStruct()) {
      throw new IllegalArgumentException("Can only unflatten a struct type.");
    }
    Node root = new Node();
    for (RelDataTypeField field : dataType.getFieldList()) {
      buildNodes(root, field.getName(), field.getType());
    }
    return buildRecord(root, typeFactory);
  }

  private static void buildNodes(Node pos, String name, RelDataType dataType) {
    if (!name.contains("$")) {
      pos.children.put(name, new Node(dataType));
    } else {
      String[] parts = name.split("\\$", 2);
      Node child = pos.children.computeIfAbsent(parts[0], x -> new Node());
      buildNodes(child, parts[1], dataType);
    }
  }

  private static RelDataType buildRecord(Node node, RelDataTypeFactory typeFactory) {
    if (node.dataType != null && !isComplexArray(node.dataType)) {
      return node.dataType;
    }
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

    // Placeholder to handle nested arrays
    if (node.children.size() == 1 && node.children.containsKey(ARRAY_TYPE)) {
      RelDataType nestedArrayType = buildRecord(node.children.get(ARRAY_TYPE), typeFactory);
      return typeFactory.createArrayType(nestedArrayType, -1);
    }
    // Placeholders to handle MAP type
    if (node.children.size() == 2
        && node.children.containsKey(MAP_KEY_TYPE) && node.children.containsKey(MAP_VALUE_TYPE)) {
      RelDataType keyType = buildRecord(node.children.get(MAP_KEY_TYPE), typeFactory);
      RelDataType valueType = buildRecord(node.children.get(MAP_VALUE_TYPE), typeFactory);
      return typeFactory.createMapType(keyType, valueType);
    }
    for (Map.Entry<String, Node> child : node.children.entrySet()) {
      builder.add(child.getKey(), buildRecord(child.getValue(), typeFactory));
    }
    if (isComplexArray(node.dataType)) {
      return typeFactory.createArrayType(builder.build(), -1);
    }
    return builder.build();
  }


  private static boolean isComplexArray(RelDataType dataType) {
    return dataType != null && dataType.getComponentType() != null
        && (dataType.getComponentType().getSqlTypeName().equals(SqlTypeName.ANY));
  }

  private static class Node {
    RelDataType dataType;
    LinkedHashMap<String, Node> children = new LinkedHashMap<>();

    Node(RelDataType dataType) {
      this.dataType = dataType;
    }

    Node() {
      // nop
    }
  }
}
