package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;

import com.linkedin.hoptimator.Validator;


/** Base class for shared schema evolution validators.  */
abstract class CompatibilityValidatorBase implements Validator<SchemaPlus> {

  @Override
  public void validate(SchemaPlus schema, Issues issues) {
    try {
      CalciteSchema originalSchema = schema.unwrap(CalciteSchema.class);
      if (originalSchema == null || originalSchema.schema == null) {
        throw new IllegalArgumentException("Null original schema (BUG)");
      }
      for (String x : schema.getTableNames()) {
        Table table = schema.getTable(x);
        Table originalTable = originalSchema.schema.getTable(x);
        if (table == null) {
          throw new IllegalArgumentException("Null table (BUG)");
        }
        if (originalTable == null) {
          continue;
        }
        validate(table, originalTable, issues.child(x));
      }
    } catch (ClassCastException e) {
      // nop
    }
  }

  protected abstract void validate(Table table, Table originalTable, Issues issues);
}
