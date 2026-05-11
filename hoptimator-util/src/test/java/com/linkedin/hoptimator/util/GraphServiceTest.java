package com.linkedin.hoptimator.util;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.PipelineGraph;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.graph.GraphTarget;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * SPI-discovery sanity checks for {@link GraphService}. Doesn't exercise a {@link
 * com.linkedin.hoptimator.graph.GraphProvider} (those live in implementation modules and need a real
 * connection); just verifies the renderer-discovery side works against the in-module Mermaid
 * renderer registered via {@code META-INF/services}, and that {@link GraphService#buildGraph}
 * fails loudly when no provider supports a target.
 */
class GraphServiceTest {

  @Test
  void availableFormatsIncludesMermaid() {
    List<String> formats = GraphService.availableFormats();
    assertTrue(formats.contains("mermaid"),
        "MermaidRenderer should be discoverable via ServiceLoader: " + formats);
  }

  @Test
  void renderUnknownFormatThrowsWithHelpfulMessage() {
    // Build the smallest valid graph to render: any non-null PipelineGraph.
    GraphNode root = new GraphNode.External("db", List.of("t"), null);
    PipelineGraph graph = new PipelineGraph(root, Set.of(root), Set.of());

    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> GraphService.render(graph, "definitely-not-a-format"));
    assertTrue(ex.getMessage().contains("definitely-not-a-format"),
        "error should mention the unknown format: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("Available"),
        "error should list available formats: " + ex.getMessage());
  }

  @Test
  void buildGraphWithNoMatchingProviderThrowsSqlException() {
    // No GraphProvider impl is on this module's test classpath, so any target should fail.
    GraphTarget unsupportedTarget = new GraphTarget.Resource("nope", List.of("nothing"));
    assertNotNull(unsupportedTarget);   // sanity guard against null returns elsewhere
    assertThrows(SQLException.class,
        () -> GraphService.buildGraph(unsupportedTarget, 1, null));
  }
}
