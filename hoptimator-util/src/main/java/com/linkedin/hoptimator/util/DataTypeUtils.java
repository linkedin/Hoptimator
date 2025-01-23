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
import org.apache.calcite.sql.type.SqlTypeName;


public final class DataTypeUtils {

  private DataTypeUtils() {
  }

  /**
   * Flattens nested structs and complex arrays.
   *
   * Nested structs like `FOO Row(BAR Row(QUX VARCHAR))` are promoted to
   * top-level fields like `FOO$BAR$QUX VARCHAR`.
   *
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
    if (dataType.getComponentType() != null && dataType.getComponentType().isStruct()) {
      builder.add(String.join("$", path), typeFactory.createArrayType(
          typeFactory.createSqlType(SqlTypeName.ANY), -1));
      for (RelDataTypeField field : dataType.getComponentType().getFieldList()) {
        flattenInto(typeFactory, field.getType(), builder, Stream.concat(path.stream(),
            Stream.of(field.getName())).collect(Collectors.toList()));
      }
    } else if (!dataType.isStruct()) {
      builder.add(String.join("$", path), dataType);
    } else {
      for (RelDataTypeField field : dataType.getFieldList()) {
        flattenInto(typeFactory, field.getType(), builder, Stream.concat(path.stream(),
            Stream.of(field.getName())).collect(Collectors.toList()));
      }
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
