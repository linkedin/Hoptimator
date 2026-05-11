package com.linkedin.hoptimator;

/**
 * SPI for serializing a {@link PipelineGraph} to a string format (Mermaid, DOT, JSON, etc.).
 * Discovered at runtime via {@code ServiceLoader}.
 *
 * <p>Each renderer declares its {@link #format()} (e.g. {@code "mermaid"}); the
 * {@code GraphService.render(graph, format)} dispatcher picks the matching renderer.
 */
public interface GraphRenderer {

  /** Serialize the graph to a string in this renderer's format. */
  String render(PipelineGraph graph);

  /** Format identifier (e.g. {@code "mermaid"}, {@code "dot"}, {@code "json"}). */
  String format();
}
