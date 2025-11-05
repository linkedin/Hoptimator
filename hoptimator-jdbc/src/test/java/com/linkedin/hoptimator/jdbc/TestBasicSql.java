package com.linkedin.hoptimator.jdbc;

import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;


public class TestBasicSql extends JdbcTestBase {

  @Test
  public void createInsertDrop() throws Exception {
    sql("CREATE TABLE T (X VARCHAR, Y VARCHAR)");
    sql("INSERT INTO T VALUES ('one', 'two')");
    assertQueriesEqual("SELECT * FROM T", "VALUES ('one', 'two')");
    sql("DROP TABLE T");
  }

  @Test
  public void insertIntoSelectFrom() throws Exception {
    sql("CREATE TABLE T1 (X VARCHAR, Y INT)");
    sql("INSERT INTO T1 VALUES ('one', 1)");
    sql("INSERT INTO T1 VALUES ('two', 2)");
    sql("CREATE TABLE T2 (A VARCHAR, B INT)");
    sql("INSERT INTO T2 SELECT * FROM T1");
    assertQueriesEqual("SELECT * FROM T1", "SELECT * FROM T2");
    assertResultSetsEqual(query("SELECT * FROM T1 WHERE X = 'one'"),
        queryUsingPreparedStatement("SELECT * FROM T1 WHERE X = ?", Arrays.asList("one")));
    sql("DROP TABLE T1");
    sql("DROP TABLE T2");
  }

  @Test
  public void createView() throws Exception {
    sql("CREATE TABLE T (X VARCHAR, Y VARCHAR)");
    sql("INSERT INTO T VALUES ('one', 'two')");
    assertQueriesEqual("SELECT * FROM T", "VALUES ('one', 'two')");
    var logs = sqlReturnsLogs("CREATE VIEW V AS SELECT * FROM T");
    assertResultSetsEqual(query("SELECT * FROM V"), query("SELECT * FROM T"));
    sql("DROP VIEW V");
    sql("DROP TABLE T");

    var expectedLogs = List.of(
        "[HoptimatorDdlExecutor] Validating statement: CREATE VIEW `V` AS\nSELECT *\nFROM `T`",
        "[HoptimatorDdlExecutor] Validated sql statement. The view is named V and has path [DEFAULT, V]",
        "[HoptimatorDdlExecutor] Validating view V with deployers",
        "[HoptimatorDdlExecutor] Validated view V",
        "[HoptimatorDdlExecutor] Deploying create view V",
        "[HoptimatorDdlExecutor] Deployed view V",
        "[HoptimatorDdlExecutor] Added view V to schema DEFAULT",
        "[HoptimatorDdlExecutor] CREATE VIEW V completed");
    Assertions.assertEquals(expectedLogs, logs);
  }

  @Test
  public void dropNonExistentViewHandlesNullSchema() throws Exception {
    // Should not throw when using IF EXISTS on a non-existent view
    sql("DROP VIEW IF EXISTS non_existing_schema.non_existing_view");

    // Should throw an Exception when dropping non-existent view without IF EXISTS
    Exception ex = Assertions.assertThrows(Exception.class, () -> {
      sql("DROP VIEW non_existing_schema.non_existing_view");
    });
    Assertions.assertTrue(
        ex.getMessage().matches("(?s).*Cannot DROP VIEW .*?: View .*? not found\\..*"),
        "Error message should match regex, but was: " + ex.getMessage()
    );
  }
}
