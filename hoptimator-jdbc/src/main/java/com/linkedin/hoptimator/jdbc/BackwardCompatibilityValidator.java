package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeUtil;


/** Validates that tables follow backwards-compatible schema evolution rules.  */
class BackwardCompatibilityValidator extends CompatibilityValidatorBase {

  BackwardCompatibilityValidator(SchemaPlus schema) {
    super(schema);
  }

  @Override
  protected void validate(Table table, Table originalTable, Issues issues) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    RelDataType originalRowType = originalTable.getRowType(typeFactory);
    for (RelDataTypeField field : rowType.getFieldList()) {
      RelDataTypeField existingField = originalRowType.getField(field.getName(), true, false);
      if (existingField == null && !field.getType().isNullable()) {
        issues.child(field.getName()).error("Backwards-incompatible change: cannot add a new non-nullable field");
      }
      if (existingField != null && !SqlTypeUtil.canAssignFrom(existingField.getType(), field.getType())) {
        String fromTypeName = existingField.getType().getSqlTypeName().getName();
        String toTypeName = field.getType().getSqlTypeName().getName();
        issues.child(field.getName())
            .error("Backwards-incompatible change: cannot assign to " + toTypeName + " from " + fromTypeName);
      }
    }
  }
}
