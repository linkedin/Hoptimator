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
   * Complex arrays are demoted to just `ANY ARRAY`. Primitive arrays are
   * unchanged.
   *
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
    if (dataType.getComponentType() != null && (dataType.getComponentType().isStruct()
        || dataType.getComponentType().getComponentType() != null)) {
      // demote complex arrays to just `ANY ARRAY`
      builder.add(path.stream().collect(Collectors.joining("$")), typeFactory.createArrayType(
          typeFactory.createSqlType(SqlTypeName.ANY), -1));
    } else if (!dataType.isStruct()) {
      builder.add(path.stream().collect(Collectors.joining("$")), dataType);
    } else {
      for (RelDataTypeField field : dataType.getFieldList()) {
        flattenInto(typeFactory, field.getType(), builder, Stream.concat(path.stream(),
            Stream.of(field.getName())).collect(Collectors.toList()));
      }
    }
  }

  /** Restructures flattened types, from `FOO$BAR VARCHAR` to `FOO Row(BAR VARCHAR...)` */
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
    if (node.dataType != null) {
      return node.dataType;
    }
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    for (Map.Entry<String, Node> child : node.children.entrySet()) {
      builder.add(child.getKey(), buildRecord(child.getValue(), typeFactory));
    }
    return builder.build();
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
