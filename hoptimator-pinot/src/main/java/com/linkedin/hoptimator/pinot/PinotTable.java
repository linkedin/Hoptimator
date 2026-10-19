package com.linkedin.hoptimator.pinot;

import com.linkedin.hoptimator.util.DataTypeUtils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Calcite table backed by a Pinot table's schema. Like the Venice adapter's table, this exposes
 * the table's row type for planning/catalog resolution only — it is not a {@code ScannableTable}
 * (data movement happens via connectors, not Calcite scans).
 *
 * <p>The row type is {@link DataTypeUtils#flatten flattened} (the JDBC-layer interop convention used
 * by the other drivers), so nested/complex columns surface as the same {@code $}-delimited scalar
 * representation the rest of the stack expects.
 */
public class PinotTable extends AbstractTable {

  private static final Logger log = LoggerFactory.getLogger(PinotTable.class);

  private final String tableName;
  private final PinotClient client;

  public PinotTable(String tableName, PinotClient client) {
    this.tableName = tableName;
    this.client = client;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    try {
      Schema schema = client.getSchema(tableName);
      if (schema == null) {
        return typeFactory.createUnknownType();
      }
      return DataTypeUtils.flatten(PinotTypeConverter.rel(schema, typeFactory), typeFactory);
    } catch (Exception e) {
      // Isolate per-table schema failures so one bad table does not fail catalog resolution.
      log.warn("Could not resolve schema for Pinot table {}", tableName, e);
      return typeFactory.createUnknownType();
    }
  }
}
