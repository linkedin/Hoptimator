package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Validator;
import java.util.EnumSet;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.checkerframework.checker.nullness.qual.Nullable;


/** Validates that no stateful operators are used in the SQL query. */
public class NoStatefulOperatorValidator implements Validator {
  private static final EnumSet<SqlKind> FORBIDDEN_KINDS = EnumSet.of(SqlKind.WINDOW,      // WINDOW clause
      SqlKind.TUMBLE,      // TUMBLE function
      SqlKind.COUNT,       // COUNT aggregate
      SqlKind.SUM         // SUM aggregate
//      String.AVG,         // AVG aggregate
  );

  private final SqlNode sqlNode;

  public NoStatefulOperatorValidator(SqlNode sqlNode) {
    this.sqlNode = sqlNode;
  }

  /**
   *
   * @param issues
   */
  @Override
  public void validate(Issues issues) {
    System.out.println("Validating that no stateful operators are used in the SQL query.");
    System.out.println("SQL Node: " + sqlNode.toString());
    SqlNodeStatefulOperatorVisitor visitor = new SqlNodeStatefulOperatorVisitor(issues);
    sqlNode.accept(visitor);
    System.out.println("Done validating no stateful operators.");
  }

  private static class SqlNodeStatefulOperatorVisitor extends SqlBasicVisitor<@Nullable SqlNode> {
    private final Issues issues;

    SqlNodeStatefulOperatorVisitor(Issues issues) {
      this.issues = issues;
    }

    @Override
    public SqlNode visit(SqlCall call) {
      System.out.println("~~~Visiting SQL call: " + call.toString());
      if (call instanceof SqlSelect) {
        // If the call is a SqlSelect, check if it contains any stateful operators.
        checkSqlSelect((SqlSelect) call);
        return null; // SqlSelect is handled separately, no need to return a node.
      }
      checkNonSelectSqlCallOperator(call);
      var ret = call.getOperator().acceptCall(this, call);
      System.out.println("~~~Finished visiting SQL call: " + call.toString());
      return ret;
    }

    private static String errorMessage(String issue, SqlNode node) {
      return "No stateful operators are allowed but found " + issue + " in SQL node: " + node.toString();
    }

    private void checkNonSelectSqlCallOperator(SqlCall call) {
      System.out.println("--Checking non-select SQL call for stateful operators: " + call);
      // Check if the SqlCall contains any stateful operators.
      // If it does, add an error to the issues.

      if (call.getOperator().isAggregator()) {
        issues.error(errorMessage("aggregator functions", call));
      }

      SqlKind operatorKind = call.getOperator().getKind();
      System.out.println("--Operator kind: " + operatorKind);
      System.out.println("--Operator name: " + call.getOperator().getName());
      if (FORBIDDEN_KINDS.contains(operatorKind)) {
        issues.error(errorMessage(operatorKind.toString(), call));
      }
      System.out.println("--Non-select SQL call check complete: " + call);
    }

    private void checkSqlSelect(SqlSelect sqlSelect) {
      System.out.println("**Checking SqlSelect for stateful operators: " + sqlSelect);
      // Check if the SqlSelect contains any stateful operators.
      // If it does, add an error to the issues.
      if (!empty(sqlSelect.getWindowList())) {
        issues.error(errorMessage("window functions", sqlSelect));
      }

      if (!empty(sqlSelect.getGroup())) {
        issues.error(errorMessage("GROUP BY clause", sqlSelect));
      }

      if (sqlSelect.getHaving() != null) {
        issues.error(errorMessage("HAVING clause", sqlSelect));
      }

      sqlSelect.getOperator().acceptCall(this, sqlSelect);

      System.out.println("**SqlSelect check complete: " + sqlSelect);
    }

    private static boolean empty(SqlNodeList nodeList) {
      return nodeList == null || nodeList.isEmpty();
    }
  }
}
