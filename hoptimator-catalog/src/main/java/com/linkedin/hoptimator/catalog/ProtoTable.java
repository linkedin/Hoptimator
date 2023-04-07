package com.linkedin.hoptimator.catalog;

import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.concurrent.ExecutionException;

/** Enables lazy-loading of AdapterTables from an Adapter */
public class ProtoTable extends AbstractTable implements TranslatableTable {
  private final String name;
  private final Adapter adapter;
  private RelDataType rowType;
  private AdapterTable table;

  ProtoTable(String name, Adapter adapter) {
    this.name = name;
    this.adapter = adapter;
  }

  /** Lazy-loads an actual Table, which may involve talking to external systems. */
  public AdapterTable table() throws InterruptedException, ExecutionException {
    if (table == null) {
      table = adapter.table(name);
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
    return new AdapterTableScan(cluster, cluster.traitSetOf(AdapterRel.CONVENTION), relOptTable);
  }
}
