package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;


public class HoptimatorJdbcTableScan extends JdbcTableScan {
  public final HoptimatorJdbcTable jdbcTable;

  protected HoptimatorJdbcTableScan(
      RelOptCluster cluster,
      List<RelHint> hints,
      RelOptTable table,
      HoptimatorJdbcTable jdbcTable,
      HoptimatorJdbcConvention jdbcConvention) {
    super(cluster, hints, table, jdbcTable.jdbcTable(), jdbcConvention);
    this.jdbcTable = jdbcTable;
  }
}
