package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.GraphTarget;
import com.linkedin.hoptimator.graph.PipelineGraph;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * Resolves {@code REFRESH} targets from the pipeline dependency graph.
 *
 * <p>REFRESH needs two facts the DDL layer can't get from a name alone: what <em>kind</em> of object
 * it is (materialized view vs logical table) and which <em>triggers</em> are immediately upstream of
 * it. Both already live in the graph layer: {@link GraphService#resolve} classifies the identifier
 * (via the Calcite schema) and {@link GraphService#buildGraph} produces the dependency graph with the
 * object's owned {@code TableTrigger}s attached. Reusing it keeps discovery decoupled from any
 * particular backend — the graph is built by a {@code GraphProvider} — without a REFRESH-specific SPI.
 */
final class RefreshService {

  private RefreshService() {
  }

  /**
   * Resolves the refresh target for {@code path}, or returns {@code null} when it isn't a
   * materialized view or logical table (an unknown name, or a plain physical resource).
   */
  static RefreshTarget resolve(List<String> path, HoptimatorConnection connection) throws SQLException {
    String identifier = String.join(".", path);

    GraphTarget target;
    try {
      target = GraphService.resolve(identifier, connection);
    } catch (SQLException e) {
      // The identifier doesn't resolve to anything this connection can see — not refreshable.
      return null;
    }

    RefreshTarget.Kind kind = kindOf(target);
    if (kind == null) {
      // A physical resource (or anything else) — not a materialized view or logical table.
      return null;
    }

    PipelineGraph graph = GraphService.buildGraph(identifier, 0, connection);
    return new RefreshTarget(kind, immediateUpstreamTriggers(graph));
  }

  /** Maps a graph target to a REFRESH kind, or {@code null} when the target isn't refreshable.
   *  {@link GraphService#resolve} only yields {@link GraphTarget.View} for a materialized view
   *  (its leaf is a {@code MaterializedViewTable}), so a plain view never reaches here. */
  static RefreshTarget.Kind kindOf(GraphTarget target) {
    if (target instanceof GraphTarget.View) {
      return RefreshTarget.Kind.MATERIALIZED_VIEW;
    }
    if (target instanceof GraphTarget.LogicalTable) {
      return RefreshTarget.Kind.TABLE;
    }
    return null;
  }

  /** Names of the trigger nodes the graph root directly owns — the triggers immediately upstream of
   *  the object (an {@code OWNER_OF} edge from the root to a {@link GraphNode.Trigger}). */
  static List<String> immediateUpstreamTriggers(PipelineGraph graph) {
    List<String> names = new ArrayList<>();
    for (GraphEdge edge : graph.edges()) {
      if (edge.type() == GraphEdge.Type.OWNER_OF
          && edge.from().equals(graph.root())
          && edge.to() instanceof GraphNode.Trigger) {
        names.add(((GraphNode.Trigger) edge.to()).name());
      }
    }
    return names;
  }
}
