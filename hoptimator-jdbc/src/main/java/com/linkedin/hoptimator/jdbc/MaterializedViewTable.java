package com.linkedin.hoptimator.jdbc;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ViewTable;

import com.linkedin.hoptimator.jdbc.schema.HoptimatorViewTableMacro;


public class MaterializedViewTable extends AbstractTable implements TranslatableTable {

  private final ViewTable viewTable;

  public MaterializedViewTable(ViewTable viewTable) {
    this.viewTable = viewTable;
  }

  public MaterializedViewTable(HoptimatorViewTableMacro viewTableMacro, Properties connectionProperties) {
    this((ViewTable) viewTableMacro.apply(Collections.singletonList(connectionProperties)));
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
