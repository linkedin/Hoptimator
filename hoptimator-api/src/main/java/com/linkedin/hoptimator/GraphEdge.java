package com.linkedin.hoptimator;

import java.util.Objects;


/**
 * A directed edge in a pipeline visualization graph. Edges are equal when their endpoints and
 * type match — two edges of different types between the same nodes are distinct (e.g. an
 * {@code ownerOf} relationship coexists with a {@code dependsOnSink} relationship).
 */
public final class GraphEdge {

  public enum Type {
    /** {@code metadata.ownerReferences} cascade — drives subgraph membership, not arrows. */
    OWNER_OF,
    /** Resource → pipeline (or job) edge derived from the {@code depends-on} annotation. */
    DEPENDS_ON_SOURCE,
    /** Pipeline (or job) → resource edge derived from the {@code depends-on} annotation. */
    DEPENDS_ON_SINK,
    /** Trigger → job/pipeline; rendered as a dotted line. */
    TRIGGERS
  }

  private final GraphNode from;
  private final GraphNode to;
  private final Type type;

  public GraphEdge(GraphNode from, GraphNode to, Type type) {
    this.from = Objects.requireNonNull(from, "from");
    this.to = Objects.requireNonNull(to, "to");
    this.type = Objects.requireNonNull(type, "type");
  }

  public GraphNode from() {
    return from;
  }

  public GraphNode to() {
    return to;
  }

  public Type type() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof GraphEdge)) {
      return false;
    }
    GraphEdge other = (GraphEdge) o;
    return type == other.type && from.equals(other.from) && to.equals(other.to);
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, to, type);
  }

  @Override
  public String toString() {
    return from.id() + " --" + type + "--> " + to.id();
  }
}
