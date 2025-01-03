package com.linkedin.hoptimator.util;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;


public class MaterializedViewTable extends AbstractTable implements TranslatableTable {

  private final ViewTable viewTable;

  public MaterializedViewTable(ViewTable viewTable) {
    this.viewTable = viewTable;
  }

  public MaterializedViewTable(ViewTableMacro viewTableMacro) {
    this((ViewTable) viewTableMacro.apply(Collections.emptyList()));
  }

  public ViewTable viewTable() {
    return viewTable;
  }

  public String viewSql() {
    return viewTable.getViewSql();
  }

  public List<String> viewPath() {
    return viewTable.getViewPath();
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return viewTable.getRowType(typeFactory);
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.MATERIALIZED_VIEW;
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    return viewTable.toRel(context, relOptTable);
  }
}
