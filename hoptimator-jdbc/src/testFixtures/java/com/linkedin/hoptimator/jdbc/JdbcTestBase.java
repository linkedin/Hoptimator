package com.linkedin.hoptimator.jdbc;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;


public abstract class JdbcTestBase {

  private static HoptimatorConnection conn;

  @BeforeAll
  public static void connect() throws Exception {
    conn = (HoptimatorConnection) DriverManager.getConnection("jdbc:hoptimator://");
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

  protected List<String> sqlReturnsLogs(String sql) throws SQLException {
    var logs = new ArrayList<String>();
    ((HoptimatorConnection) conn).addLogHook(logs::add);
    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(sql);
    }
    return logs;
  }

  protected void assertQueriesEqual(String q1, String q2) throws SQLException {
    assertResultSetsEqual(query(q1), query(q2));
  }

  protected void assertResultSetsEqual(List<Object[]> res1, List<Object[]> res2) {
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
    Assertions.assertFalse(res.isEmpty(), "ResultSet is empty");
  }

  protected List<Object[]> query(String query) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      ResultSet cursor = stmt.executeQuery(query);
      return buildRows(cursor);
    }
  }

  protected List<Object[]> queryUsingPreparedStatement(String query, List<String> params) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(query)) {
      for (int i = 0; i < params.size(); i++) {
        stmt.setString(i + 1, params.get(i));
      }
      ResultSet cursor = stmt.executeQuery();
      return buildRows(cursor);
    }
  }

  private static List<Object[]> buildRows(ResultSet cursor) throws SQLException {
    List<Object[]> results = new ArrayList<>();
    while (cursor.next()) {
      int n = cursor.getMetaData().getColumnCount();
      Object[] row = new Object[n];
      for (int i = 0; i < n; i++) {
        row[i] = cursor.getObject(i + 1);
      }
      results.add(row);
    }
    return results;
  }

  /**
   * Validates that a table exists in the specified schema with expected columns.
   * This avoids querying metadata.columns which requires loading all drivers.
   *
   * @param path the path to the table (e.g., ["KAFKA", "my_table"])
   * @param expectedColumns map of column name to expected SQL type name
   */
  protected void validateTableSchema(List<String> path, Map<String, String> expectedColumns) {
    Schema rootSchema = conn.calciteConnection().getRootSchema();

    String tableName = path.get(path.size() - 1);

    // Navigate to the target schema
    Schema schema = rootSchema;
    for (int i = 0; i < path.size() - 1; i++) {
      schema = Objects.requireNonNull(schema.subSchemas().get(path.get(i)));
    }
    Assertions.assertNotNull(schema, "Schema '" + path + "' should exist");

    // Get the table
    Table table = schema.tables().get(tableName);
    Assertions.assertNotNull(table, "Table '" + tableName + "' should exist in schema '" + schema + "'");

    // Validate table structure
    RelDataType rowType = table.getRowType(conn.calciteConnection().getTypeFactory());
    List<RelDataTypeField> fields = rowType.getFieldList();

    Assertions.assertEquals(expectedColumns.size(), fields.size(),
        "Table should have " + expectedColumns.size() + " columns");

    for (RelDataTypeField field : fields) {
      String columnName = field.getName();
      String actualType = field.getType().getSqlTypeName().getName();

      Assertions.assertTrue(expectedColumns.containsKey(columnName),
          "Unexpected column: " + columnName);

      String expectedType = expectedColumns.get(columnName);
      Assertions.assertEquals(expectedType, actualType,
          "Column '" + columnName + "' should have type '" + expectedType + "'");
    }
  }
}
