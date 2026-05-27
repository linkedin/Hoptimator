package com.linkedin.hoptimator.planner;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class SqlJobTest {

  @Test
  public void constructorSetsTemplateToSqlJob() {
    SqlJob sqlJob = new SqlJob("SELECT 1");
    assertEquals("SqlJob", sqlJob.template());
  }

  @Test
  public void constructorExportsPipelineSqlProperty() {
    SqlJob sqlJob = new SqlJob("SELECT 1");
    assertEquals("SELECT 1", sqlJob.property("pipeline.sql"));
  }

  @Test
  public void constructorWithComplexSqlExportsCorrectly() {
    String sql = "INSERT INTO sink SELECT a, b FROM source WHERE a > 0";
    SqlJob sqlJob = new SqlJob(sql);
    assertEquals(sql, sqlJob.property("pipeline.sql"));
  }

  @Test
  public void sqlJobHasIdProperty() {
    SqlJob sqlJob = new SqlJob("SELECT 1");
    assertNotNull(sqlJob.property("id"));
  }

  @Test
  public void twoSqlJobsWithSameSqlHaveSameHashCode() {
    SqlJob a = new SqlJob("SELECT 1");
    SqlJob b = new SqlJob("SELECT 1");
    // hashCode is based on toString() which iterates properties — both have same pipeline.sql
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void keysContainsPipelineSqlAndId() {
    SqlJob sqlJob = new SqlJob("SELECT 1");
    assert sqlJob.keys().contains("pipeline.sql");
    assert sqlJob.keys().contains("id");
  }
}
