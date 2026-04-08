package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Engine;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class EngineRulesTest {

  @Mock
  private Engine mockEngine;

  @Mock
  private RelOptPlanner mockPlanner;

  @Mock
  private Connection mockConnection;

  @Mock
  private Expression mockExpression;

  @Test
  void testRegisterAddsRulesToPlanner() {
    when(mockEngine.engineName()).thenReturn("flink");

    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "db", Collections.emptyList(), mockConnection);

    EngineRules rules = new EngineRules(mockEngine);
    rules.register(convention, mockPlanner, mockConnection);

    verify(mockPlanner, atLeastOnce()).addRule(any());
  }

  @Test
  void testConstructor() {
    EngineRules rules = new EngineRules(mockEngine);

    assertNotNull(rules);
  }

  @Test
  void testDialectAnsiReturnsAnsiDialect() {
    when(mockEngine.dialect()).thenReturn(com.linkedin.hoptimator.SqlDialect.ANSI);

    SqlDialect result = EngineRules.dialect(mockEngine);

    assertEquals(AnsiSqlDialect.DEFAULT, result);
  }

  @Test
  void testDialectFlinkReturnsMysqlDialect() {
    when(mockEngine.dialect()).thenReturn(com.linkedin.hoptimator.SqlDialect.FLINK);

    SqlDialect result = EngineRules.dialect(mockEngine);

    assertEquals(MysqlSqlDialect.DEFAULT, result);
  }
}
