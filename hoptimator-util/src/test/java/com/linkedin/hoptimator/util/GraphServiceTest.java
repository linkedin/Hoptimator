package com.linkedin.hoptimator.util;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.GraphTarget;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * SPI-discovery sanity checks for {@link GraphService}. Doesn't exercise a {@link
 * com.linkedin.hoptimator.GraphProvider} (those live in implementation modules and need a real
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
    com.linkedin.hoptimator.GraphNode root =
        new com.linkedin.hoptimator.GraphNode.External("db", List.of("t"), null);
    com.linkedin.hoptimator.PipelineGraph graph = new com.linkedin.hoptimator.PipelineGraph(
        root, java.util.Set.of(root), java.util.Set.of());

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
    assertThrows(java.sql.SQLException.class,
        () -> GraphService.buildGraph(unsupportedTarget, 1, null));
  }
}
