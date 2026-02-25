package com.linkedin.hoptimator.k8s;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.linkedin.hoptimator.ConnectorConfigurable;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableSpecColumn;


/**
 * A Calcite table backed by a K8s Table CRD.
 * Loaded from stored column definitions and connector options.
 * Does NOT implement TranslatableTable; Calcite creates a LogicalTableScan,
 * which StoredTableScanRule converts to PipelineTableScan.
 */
class K8sStoredTable extends AbstractTable implements ConnectorConfigurable {

  private final String databaseName;
  private final List<V1alpha1TableSpecColumn> columns;
  private final Map<String, String> options;

  K8sStoredTable(String databaseName, List<V1alpha1TableSpecColumn> columns,
      Map<String, String> options) {
    this.databaseName = databaseName;
    this.columns = columns != null ? columns : Collections.emptyList();
    this.options = options != null ? options : Collections.emptyMap();
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (V1alpha1TableSpecColumn col : columns) {
      SqlTypeName typeName = mapType(col.getType());
      RelDataType type = typeFactory.createSqlType(typeName);
      boolean nullable = col.getNullable() == null || col.getNullable();
      type = typeFactory.createTypeWithNullability(type, nullable);
      builder.add(col.getName(), type);
    }
    return builder.build();
  }

  @Override
  public Map<String, String> connectorOptions() {
    return options;
  }

  @Override
  public String databaseName() {
    return databaseName;
  }

  @Override
  public <C> C unwrap(Class<C> aClass) {
    if (aClass.isInstance(this)) {
      return aClass.cast(this);
    }
    return super.unwrap(aClass);
  }

  private static SqlTypeName mapType(String typeName) {
    if (typeName == null) {
      return SqlTypeName.VARCHAR;
    }
    try {
      return SqlTypeName.valueOf(typeName.toUpperCase());
    } catch (IllegalArgumentException e) {
      // Handle common aliases
      switch (typeName.toUpperCase()) {
        case "INT":
          return SqlTypeName.INTEGER;
        case "STRING":
        case "TEXT":
          return SqlTypeName.VARCHAR;
        case "LONG":
          return SqlTypeName.BIGINT;
        case "BOOL":
          return SqlTypeName.BOOLEAN;
        case "BYTES":
          return SqlTypeName.VARBINARY;
        default:
          return SqlTypeName.VARCHAR;
      }
    }
  }
}
