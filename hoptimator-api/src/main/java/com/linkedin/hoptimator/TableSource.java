package com.linkedin.hoptimator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A Source that carries column definitions alongside options.
 * Used for CREATE TABLE ... WITH (...) to store ad-hoc table definitions in K8s.
 */
public class TableSource extends Source {

  private final List<ColumnDefinition> columns;

  public TableSource(String database, List<String> path, Map<String, String> options,
      List<ColumnDefinition> columns) {
    super(database, path, options);
    this.columns = columns != null ? Collections.unmodifiableList(columns) : Collections.emptyList();
  }

  public List<ColumnDefinition> columns() {
    return columns;
  }

  /**
   * Represents a column definition from a CREATE TABLE statement.
   */
  public static class ColumnDefinition {
    private final String name;
    private final String typeName;
    private final boolean nullable;

    public ColumnDefinition(String name, String typeName, boolean nullable) {
      this.name = name;
      this.typeName = typeName;
      this.nullable = nullable;
    }

    public String name() {
      return name;
    }

    public String typeName() {
      return typeName;
    }

    public boolean nullable() {
      return nullable;
    }

    @Override
    public String toString() {
      return name + " " + typeName + (nullable ? "" : " NOT NULL");
    }
  }

  @Override
  public String toString() {
    return "TableSource[" + pathString() + "]";
  }
}
