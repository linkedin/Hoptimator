package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static org.apache.calcite.linq4j.Nullness.castNonNull;


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
