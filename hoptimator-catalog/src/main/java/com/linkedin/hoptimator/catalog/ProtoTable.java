package com.linkedin.hoptimator.catalog;

import java.util.concurrent.ExecutionException;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;


/** Enables lazy-loading of HopTables */
public class ProtoTable extends AbstractTable implements TranslatableTable {
  private final String name;
  private final Database database;
  private HopTable table;

  ProtoTable(String name, Database database) {
    this.name = name;
    this.database = database;
  }

  /** Lazy-loads an actual Table, which may involve talking to external systems. */
  public HopTable table() throws InterruptedException, ExecutionException {
    if (table == null) {
      table = database.table(name);
    }
    return table;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    try {
      return table().getRowType(typeFactory);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    RelOptCluster cluster = context.getCluster();
    return new HopTableScan(cluster, cluster.traitSetOf(HopRel.CONVENTION), relOptTable);
  }
}
