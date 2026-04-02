package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;


@ExtendWith(MockitoExtension.class)
class HoptimatorJdbcTableScanTest {

  @Mock
  private JdbcTable mockJdbcTable;

  @Mock
  private Connection mockConnection;

  @Mock
  private RelOptTable mockRelOptTable;

  @Test
  void testConstructorStoresJdbcTable() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    Expression expression = Expressions.call(Expressions.new_(Object.class), "toString");
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, expression, "testDb", Collections.emptyList(), mockConnection);

    HoptimatorJdbcTable hoptimatorJdbcTable = new HoptimatorJdbcTable(mockJdbcTable, convention);

    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

    HoptimatorJdbcTableScan scan = new HoptimatorJdbcTableScan(
        cluster, Collections.emptyList(), mockRelOptTable, hoptimatorJdbcTable, convention);

    assertNotNull(scan);
    assertSame(hoptimatorJdbcTable, scan.jdbcTable);
  }
}
