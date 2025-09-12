package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlItemOperator;


/**
 * RelShuttle implementation that checks if a query is trivial.
 * A trivial query only contains TableScans and simple LogicalProjects.
 */
public class TrivialQueryChecker implements RelShuttle {
  private boolean trivial = true;

  /**
   * Determines if a RelNode represents a trivial query that only contains:
   * - Simple table scans
   * - Simple projections (field references and aliasing)
   * - No joins, aggregations, filters, functions, or other complex operations
   */
  public static boolean isTrivialQuery(RelNode node) {
    TrivialQueryChecker checker = new TrivialQueryChecker();
    try {
      node.accept(checker);
      return checker.isTrivial();
    } catch (Exception e) {
      return false;
    }
  }

  public boolean isTrivial() {
    return trivial;
  }

  @Override
  public RelNode visit(TableScan scan) {
    return visitScan(scan);
  }

  @Override
  public RelNode visit(LogicalProject project) {
    return visitProject(project);
  }

  @Override
  public RelNode visit(TableFunctionScan scan) {
    trivial = false; // Not allowed in trivial queries
    return scan;
  }

  @Override
  public RelNode visit(LogicalValues values) {
    trivial = false; // Not allowed in trivial queries
    return values;
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    trivial = false; // Not allowed in trivial queries
    return filter;
  }

  @Override
  public RelNode visit(LogicalCalc calc) {
    trivial = false; // Not allowed in trivial queries
    return calc;
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    trivial = false; // Not allowed in trivial queries
    return join;
  }

  @Override
  public RelNode visit(LogicalCorrelate correlate) {
    trivial = false; // Not allowed in trivial queries
    return correlate;
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    trivial = false; // Not allowed in trivial queries
    return union;
  }

  @Override
  public RelNode visit(LogicalIntersect intersect) {
    trivial = false; // Not allowed in trivial queries
    return intersect;
  }

  @Override
  public RelNode visit(LogicalMinus minus) {
    trivial = false; // Not allowed in trivial queries
    return minus;
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    trivial = false; // Not allowed in trivial queries
    return aggregate;
  }

  @Override
  public RelNode visit(LogicalMatch match) {
    trivial = false; // Not allowed in trivial queries
    return match;
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    trivial = false; // Not allowed in trivial queries
    return sort;
  }

  @Override
  public RelNode visit(LogicalExchange exchange) {
    trivial = false; // Not allowed in trivial queries
    return exchange;
  }

  @Override
  public RelNode visit(LogicalTableModify modify) {
    trivial = false; // Not allowed in trivial queries
    return modify;
  }

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof PipelineRules.PipelineProject) {
      return visitProject((PipelineRules.PipelineProject) other);
    } else if (other instanceof PipelineRules.PipelineTableScan) {
      return visitScan((PipelineRules.PipelineTableScan) other);
    }

    // All other node types not specified above are considered non-trivial
    trivial = false;
    return other;
  }

  private RelNode visitScan(TableScan scan) {
    // Table scans are allowed in trivial queries
    return scan;
  }

  private RelNode visitProject(Project project) {
    // Check if the project only contains simple field references
    for (RexNode expr : project.getProjects()) {
      if (!isSimpleFieldReference(expr)) {
        trivial = false;
        break;
      }
    }
    // Continue visiting children
    return visitChildren(project);
  }

  /**
   * Checks if a RexNode represents a simple field reference (no functions, calculations, etc.)
   * This includes:
   * - Simple field references by index (RexInputRef)
   * - Nested field access using ITEM operator (e.g., ITEM($3, 'nestedField'))
   */
  private boolean isSimpleFieldReference(RexNode expr) {
    if (expr instanceof RexInputRef) {
      return true; // Simple field reference by index
    }

    if (expr instanceof RexCall) {
      RexCall call = (RexCall) expr;

      // Handle ITEM operator for nested field access: ITEM($field, 'nestedField')
      if (call.getOperator() instanceof SqlItemOperator && call.getOperands().size() == 2) {
        RexNode baseField = call.getOperands().get(0);
        RexNode nestedFieldLiteral = call.getOperands().get(1);

        // The base field should be a simple field reference
        // The nested field should be a string literal
        return isSimpleFieldReference(baseField) && nestedFieldLiteral instanceof RexLiteral;
      }
    }

    return false;
  }

  /**
   * Visits all children of a RelNode
   */
  private RelNode visitChildren(RelNode node) {
    for (RelNode input : node.getInputs()) {
      input.accept(this);
      if (!trivial) {
        break; // Short-circuit if we already found it's not trivial
      }
    }
    return node;
  }
}
