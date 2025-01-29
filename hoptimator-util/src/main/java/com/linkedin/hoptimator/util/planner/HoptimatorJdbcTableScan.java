package com.linkedin.hoptimator.util.planner;

import java.util.List;

import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;


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
