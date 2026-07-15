package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.GraphTarget;
import com.linkedin.hoptimator.graph.PipelineGraph;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;


class RefreshServiceTest {

  private static GraphNode.Trigger trigger(String name) {
    return new GraphNode.Trigger(name, null, true, null, null);
  }

  @Test
  void kindOfMapsViewToMaterializedView() {
    assertThat(RefreshService.kindOf(new GraphTarget.View("mv")))
        .isEqualTo(RefreshTarget.Kind.MATERIALIZED_VIEW);
  }

  @Test
  void kindOfMapsLogicalTableToTable() {
    assertThat(RefreshService.kindOf(new GraphTarget.LogicalTable("lt")))
        .isEqualTo(RefreshTarget.Kind.TABLE);
  }

  @Test
  void kindOfRejectsPlainResource() {
    assertThat(RefreshService.kindOf(new GraphTarget.Resource("db", Arrays.asList("t"))))
        .isNull();
  }

  @Test
  void immediateUpstreamTriggersReturnsOnlyRootOwnedTriggers() {
    GraphNode.LogicalTable root = new GraphNode.LogicalTable("lt", Collections.emptyMap());
    GraphNode.Trigger owned = trigger("owned-trigger");
    GraphNode.Trigger notOwned = trigger("other-trigger");
    GraphNode.Pipeline pipeline = new GraphNode.Pipeline("p", null, null, null);
    GraphNode.External resource = new GraphNode.External("db", Arrays.asList("R"));

    Set<GraphNode> nodes = new LinkedHashSet<>(Arrays.asList(root, owned, notOwned, pipeline, resource));
    Set<GraphEdge> edges = new LinkedHashSet<>(Arrays.asList(
        new GraphEdge(root, owned, GraphEdge.Type.OWNER_OF),        // counts
        new GraphEdge(root, pipeline, GraphEdge.Type.OWNER_OF),     // not a trigger
        new GraphEdge(resource, owned, GraphEdge.Type.TRIGGERS),    // wrong edge type
        new GraphEdge(resource, notOwned, GraphEdge.Type.TRIGGERS)  // not owned by root
    ));
    PipelineGraph graph = new PipelineGraph(root, nodes, edges);

    List<String> names = RefreshService.immediateUpstreamTriggers(graph);

    assertThat(names).containsExactly("owned-trigger");
  }

  @Test
  void immediateUpstreamTriggersEmptyWhenRootOwnsNoTriggers() {
    GraphNode.View root = new GraphNode.View("mv", true);
    GraphNode.Pipeline pipeline = new GraphNode.Pipeline("p", null, null, null);
    Set<GraphNode> nodes = new LinkedHashSet<>(Arrays.asList(root, pipeline));
    Set<GraphEdge> edges = Collections.singleton(
        new GraphEdge(root, pipeline, GraphEdge.Type.OWNER_OF));
    PipelineGraph graph = new PipelineGraph(root, nodes, edges);

    assertThat(RefreshService.immediateUpstreamTriggers(graph)).isEmpty();
  }
}
