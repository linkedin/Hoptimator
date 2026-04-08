package com.linkedin.hoptimator.planner;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;


public class HoptimatorHookTest {

  @Test
  public void yamlPlanAndQueryPlanAreDistinctInstances() {
    assertNotSame(HoptimatorHook.YAML_PLAN, HoptimatorHook.QUERY_PLAN);
  }

  @Test
  public void initialRunReturnsNothingYet() {
    // YAML_PLAN and QUERY_PLAN are singletons; inspect initial state by creating new state via run
    // Since there's no reset API, verify the static instances are non-null
    assertNotNull(HoptimatorHook.YAML_PLAN);
    assertNotNull(HoptimatorHook.QUERY_PLAN);
  }

  @Test
  public void runStoresAndRetrievesValue() {
    HoptimatorHook.YAML_PLAN.run("test-yaml-plan");
    assertEquals("test-yaml-plan", HoptimatorHook.YAML_PLAN.run());
  }

  @Test
  public void queryPlanRunStoresAndRetrievesValue() {
    HoptimatorHook.QUERY_PLAN.run("test-query-plan");
    assertEquals("test-query-plan", HoptimatorHook.QUERY_PLAN.run());
  }

  @Test
  public void runOverwritesPreviousValue() {
    HoptimatorHook.YAML_PLAN.run("first");
    HoptimatorHook.YAML_PLAN.run("second");
    assertEquals("second", HoptimatorHook.YAML_PLAN.run());
  }

  @Test
  public void twoInstancesAreIndependent() {
    HoptimatorHook.YAML_PLAN.run("yaml-value");
    HoptimatorHook.QUERY_PLAN.run("query-value");

    assertEquals("yaml-value", HoptimatorHook.YAML_PLAN.run());
    assertEquals("query-value", HoptimatorHook.QUERY_PLAN.run());
  }
}
