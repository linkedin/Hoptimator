package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeUtil;

/** Validates that tables follow forwards-compatible schema evolution rules.  */
class ForwardCompatibilityValidator extends CompatibilityValidatorBase {
  
  @Override
  protected void validate(Table table, Table originalTable, Issues issues) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    RelDataType rowType = table.getRowType(typeFactory);
    RelDataType originalRowType = originalTable.getRowType(typeFactory);
    for (RelDataTypeField field : originalRowType.getFieldList()) {
      RelDataTypeField newField = rowType.getField(field.getName(), true, false);
      if (newField == null && !field.getType().isNullable()) {
        issues.child(field.getName()).error("Forwards-incompatible change: cannot remove a new non-nullable field");
      }
      if (newField != null && !SqlTypeUtil.canAssignFrom(newField.getType(), field.getType())) {
        String fromTypeName = newField.getType().getSqlTypeName().getName();
        String toTypeName = field.getType().getSqlTypeName().getName();
        issues.child(field.getName()).error("Forwards-incompatible change: cannot assign to "
            + toTypeName + " from " + fromTypeName);
      }
    }
  }
}
