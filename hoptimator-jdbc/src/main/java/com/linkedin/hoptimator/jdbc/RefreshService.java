package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.GraphTarget;
import com.linkedin.hoptimator.graph.PipelineGraph;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * Resolves the trigger(s) a {@code REFRESH} should fire — the trigger(s) that <em>produce</em>
 * the named physical table.
 *
 * <p>Hoptimator doesn't distinguish logical from physical tables, and a consumer always reads a
 * specific physical table, so REFRESH targets a physical table and fires whatever writes to it.
 * Discovery reuses the pipeline dependency graph: {@link GraphService#resolve} classifies the
 * identifier via the Calcite schema, and the one-hop graph around it exposes the producing triggers
 * as {@code trigger -> table} ({@link GraphEdge.Type#TRIGGERS}) edges. In practice a physical table
 * has zero or one producing trigger. The DDL layer never touches Kubernetes — the graph is built by
 * a pluggable {@code GraphProvider}.
 */
final class RefreshService {

  private RefreshService() {
  }

  /**
   * Returns the names of the triggers that produce {@code path} (usually zero or one). Throws when
   * the identifier doesn't resolve to a table, or resolves to a logical table (which has no single
   * physical output to refresh — a caller should refresh a specific tier instead).
   */
  static List<String> producingTriggers(List<String> path, HoptimatorConnection connection)
      throws SQLException {
    String identifier = String.join(".", path);
    GraphTarget target = GraphService.resolve(identifier, connection);
    if (target instanceof GraphTarget.LogicalTable) {
      throw new SQLException(identifier + " is a logical table; REFRESH a specific physical table "
          + "(tier) instead.");
    }
    PipelineGraph graph = GraphService.buildGraph(target, 1, connection);
    return producingTriggers(graph);
  }

  /** Names of the triggers that produce the graph's root table — {@code trigger -> root}
   *  ({@link GraphEdge.Type#TRIGGERS}) edges. Consumer edges ({@code root -> trigger}) are ignored. */
  static List<String> producingTriggers(PipelineGraph graph) {
    List<String> names = new ArrayList<>();
    for (GraphEdge edge : graph.edges()) {
      if (edge.type() == GraphEdge.Type.TRIGGERS
          && edge.to().equals(graph.root())
          && edge.from() instanceof GraphNode.Trigger) {
        names.add(((GraphNode.Trigger) edge.from()).name());
      }
    }
    return names;
  }
}
