package com.linkedin.hoptimator.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;


public abstract class JdbcTestBase {

  private static Connection conn;

  @BeforeAll
  public static void connect() throws Exception {
    conn = DriverManager.getConnection("jdbc:hoptimator://");
  }

  @AfterAll
  public static void disconnect() throws Exception {
    conn.close();
  }

  protected void sql(String sql) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
    }
  }

  protected void assertQueriesEqual(String q1, String q2) throws SQLException {
    List<Object[]> res1 = query(q1);
    List<Object[]> res2 = query(q2);
    Assertions.assertEquals(res1.size(), res2.size(), "ResultSets are not the same size");
    for (int i = 0; i < res1.size(); i++) {
      Assertions.assertArrayEquals(res1.get(i), res2.get(i), "Rows did not match at row #" + i);
    }
  }

  protected void assertQueryEmpty(String q) throws SQLException {
    List<Object[]> res = query(q);
    Assertions.assertTrue(res.isEmpty(), "ResultSet is not empty");
  }

  protected void assertQueryNonEmpty(String q) throws SQLException {
    List<Object[]> res = query(q);
    Assertions.assertTrue(!res.isEmpty(), "ResultSet is empty");
  }

  protected List<Object[]> query(String query) throws SQLException {
    List<Object[]> results = new ArrayList<>();
    try (Statement stmt = conn.createStatement()) {
      ResultSet cursor = stmt.executeQuery(query);
      while (cursor.next()) {
        int n = cursor.getMetaData().getColumnCount();
        Object[] row = new Object[n];
        for (int i = 0; i < n; i++) {
          row[i] = cursor.getObject(i + 1);
        }
        results.add(row);
      }
    }
    return results;
  }
}
