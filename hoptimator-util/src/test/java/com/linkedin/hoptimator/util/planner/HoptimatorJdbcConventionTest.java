package com.linkedin.hoptimator.util.planner;

import com.linkedin.hoptimator.Engine;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class HoptimatorJdbcConventionTest {

  @Mock
  private Expression mockExpression;

  @Mock
  private Connection mockConnection;

  @Mock
  private Engine mockEngine;

  @Mock
  private RelOptPlanner mockPlanner;

  @Test
  void testDatabaseReturnsName() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);

    assertEquals("myDb", convention.database());
  }

  @Test
  void testEnginesReturnsProvidedList() {
    List<Engine> engines = List.of(mockEngine);
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", engines, mockConnection);

    assertEquals(1, convention.engines().size());
    assertSame(mockEngine, convention.engines().get(0));
  }

  @Test
  void testRemoteConventionForEngineCachesResult() {
    when(mockEngine.engineName()).thenReturn("flink");
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);

    RemoteConvention remote1 = convention.remoteConventionForEngine(mockEngine);
    RemoteConvention remote2 = convention.remoteConventionForEngine(mockEngine);

    assertNotNull(remote1);
    assertSame(remote1, remote2);
    assertEquals(mockEngine, remote1.engine());
  }

  @Test
  void testRegisterAddsRulesToPlanner() {
    when(mockEngine.engineName()).thenReturn("flink");
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", List.of(mockEngine), mockConnection);

    convention.register(mockPlanner);

    verify(mockPlanner, atLeastOnce()).addRule(any());
  }

  // Verify that register() adds at least the PipelineRules.rules() count + 2 specific rules.
  @Test
  void testRegisterAddsAtLeastPipelineRulesCount() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);

    convention.register(mockPlanner);

    // PipelineRules.rules() has 6 rules, plus PipelineTableScanRule and
    // PipelineTableModifyRule. So at least 8 addRule calls from hoptimator code alone.
    int expectedMinimum = PipelineRules.rules().size() + 2;
    verify(mockPlanner, atLeast(expectedMinimum)).addRule(any());
  }

  // --- register() returns the convention object (sanity check) ---
  @Test
  void testRegisterWithEmptyEnginesDoesNotThrow() {
    HoptimatorJdbcConvention convention = new HoptimatorJdbcConvention(
        AnsiSqlDialect.DEFAULT, mockExpression, "myDb", Collections.emptyList(), mockConnection);

    // Should not throw
    convention.register(mockPlanner);
    assertNotNull(convention);
  }
}
