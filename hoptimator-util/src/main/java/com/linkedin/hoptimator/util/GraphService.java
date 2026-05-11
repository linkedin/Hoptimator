package com.linkedin.hoptimator.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.GraphProvider;
import com.linkedin.hoptimator.GraphRenderer;
import com.linkedin.hoptimator.GraphTarget;
import com.linkedin.hoptimator.PipelineGraph;


/**
 * SPI dispatcher for {@link GraphProvider} (graph construction) and {@link GraphRenderer}
 * (graph serialization). Mirrors the {@code DeploymentService} pattern: providers and
 * renderers register via {@code META-INF/services}, callers go through static methods on
 * this class to discover and dispatch.
 */
public final class GraphService {

  private GraphService() {
  }

  /**
   * Build a {@link PipelineGraph} for the given target by dispatching to the first registered
   * {@link GraphProvider} that {@code supports(target)}.
   *
   * @throws SQLException if no provider supports the target, or if the underlying provider
   *                      throws.
   */
  public static PipelineGraph buildGraph(GraphTarget target, int depth, Connection connection)
      throws SQLException {
    for (GraphProvider provider : providers()) {
      if (provider.supports(target)) {
        return provider.forTarget(target, depth, connection);
      }
    }
    throw new SQLException("No GraphProvider supports target: " + target
        + ". Registered providers: " + providerSummary());
  }

  /**
   * Render a graph using the registered {@link GraphRenderer} whose {@link GraphRenderer#format()}
   * matches {@code format} (case-insensitive).
   *
   * @throws IllegalArgumentException if no renderer supports {@code format}.
   */
  public static String render(PipelineGraph graph, String format) {
    for (GraphRenderer renderer : renderers()) {
      if (renderer.format().equalsIgnoreCase(format)) {
        return renderer.render(graph);
      }
    }
    throw new IllegalArgumentException("No GraphRenderer registered for format: " + format
        + ". Available: " + availableFormats());
  }

  /** Format identifiers (e.g. {@code mermaid}) registered by visible {@link GraphRenderer}s. */
  public static List<String> availableFormats() {
    return renderers().stream()
        .map(GraphRenderer::format)
        .distinct()
        .collect(Collectors.toList());
  }

  // ─── SPI loading ─────────────────────────────────────────────────────────

  private static List<GraphProvider> providers() {
    List<GraphProvider> providers = new ArrayList<>();
    ServiceLoader.load(GraphProvider.class).iterator().forEachRemaining(providers::add);
    return providers;
  }

  private static List<GraphRenderer> renderers() {
    List<GraphRenderer> renderers = new ArrayList<>();
    ServiceLoader.load(GraphRenderer.class).iterator().forEachRemaining(renderers::add);
    return renderers;
  }

  private static String providerSummary() {
    List<String> names = new ArrayList<>();
    for (GraphProvider p : providers()) {
      names.add(p.getClass().getSimpleName());
    }
    return names.isEmpty() ? "<none>" : String.join(", ", names);
  }
}
