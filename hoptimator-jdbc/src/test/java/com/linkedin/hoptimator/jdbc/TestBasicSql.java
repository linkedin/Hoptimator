package com.linkedin.hoptimator.jdbc;

import org.junit.jupiter.api.Test;


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
    sql("DROP TABLE T1");
    sql("DROP TABLE T2");
  }
}
