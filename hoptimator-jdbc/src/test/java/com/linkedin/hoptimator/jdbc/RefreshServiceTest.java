package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.PipelineGraph;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;


class RefreshServiceTest {

  private static GraphNode.Trigger trigger(String name) {
    return new GraphNode.Trigger(name, null, true, null, null);
  }

  private static GraphNode.External ext(String db, String name) {
    return new GraphNode.External(db, Arrays.asList(name));
  }

  private static PipelineGraph graph(GraphNode root, GraphEdge... edges) {
    Set<GraphNode> nodes = new LinkedHashSet<>();
    nodes.add(root);
    for (GraphEdge e : edges) {
      nodes.add(e.from());
      nodes.add(e.to());
    }
    return new PipelineGraph(root, nodes, new LinkedHashSet<>(Arrays.asList(edges)));
  }

  @Test
  void findsTriggersThatProduceTheTable() {
    // R is the refreshed table; t1 produces it (t1 -> R). A consumer edge (R -> t2) must be ignored.
    GraphNode.External r = ext("ads-database", "MEMBERS");
    GraphNode.Trigger t1 = trigger("producer");
    GraphNode.Trigger t2 = trigger("consumer");
    PipelineGraph g = graph(r,
        new GraphEdge(t1, r, GraphEdge.Type.TRIGGERS),   // t1 produces R -> fired
        new GraphEdge(r, t2, GraphEdge.Type.TRIGGERS));  // t2 consumes R -> not fired

    assertThat(RefreshService.producingTriggers(g)).containsExactly("producer");
  }

  @Test
  void emptyWhenNothingProducesTheTable() {
    GraphNode.External r = ext("ads-database", "MEMBERS");
    GraphNode.Trigger t2 = trigger("consumer");
    PipelineGraph g = graph(r, new GraphEdge(r, t2, GraphEdge.Type.TRIGGERS));

    assertThat(RefreshService.producingTriggers(g)).isEmpty();
  }

  @Test
  void ignoresNonTriggerAndNonRootEdges() {
    GraphNode.External r = ext("ads-database", "MEMBERS");
    GraphNode.External other = ext("ads-database", "OTHER");
    GraphNode.Trigger t = trigger("elsewhere");
    // A producer edge into a different resource must not count for R.
    PipelineGraph g = graph(r,
        new GraphEdge(t, other, GraphEdge.Type.TRIGGERS),
        new GraphEdge(r, other, GraphEdge.Type.DEPENDS_ON_SINK));

    assertThat(RefreshService.producingTriggers(g)).isEmpty();
  }
}
