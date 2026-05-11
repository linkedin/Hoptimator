package com.linkedin.hoptimator.graph;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;


/**
 * A pipeline visualization graph: a {@link #root()} node the user asked about plus the surrounding
 * nodes and edges discovered during traversal.
 *
 * <p>The graph is intentionally minimal — discovery lives in builders (e.g.
 * {@code PipelineGraphBuilder} in hoptimator-k8s) and rendering lives in renderers (e.g.
 * {@code MermaidRenderer} in hoptimator-graph). This POJO is the wire format between them.
 */
public final class PipelineGraph {

  private final GraphNode root;
  private final Set<GraphNode> nodes;
  private final Set<GraphEdge> edges;

  public PipelineGraph(GraphNode root, Set<GraphNode> nodes, Set<GraphEdge> edges) {
    this.root = Objects.requireNonNull(root, "root");
    this.nodes = Collections.unmodifiableSet(new LinkedHashSet<>(Objects.requireNonNull(nodes, "nodes")));
    this.edges = Collections.unmodifiableSet(new LinkedHashSet<>(Objects.requireNonNull(edges, "edges")));
    if (!this.nodes.contains(root)) {
      throw new IllegalArgumentException("root node " + root.id() + " is not in the node set");
    }
  }

  /** The node the user asked the graph to be built around (the entity highlighted by renderers). */
  public GraphNode root() {
    return root;
  }

  public Set<GraphNode> nodes() {
    return nodes;
  }

  public Set<GraphEdge> edges() {
    return edges;
  }
}
