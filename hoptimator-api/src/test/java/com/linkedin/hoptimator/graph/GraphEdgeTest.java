package com.linkedin.hoptimator.graph;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Equality semantics for {@link GraphEdge}. The graph stores edges in a {@code Set}; if two
 * edges with the same endpoints + type aren't equal, we'd end up with duplicate arrows in
 * the rendered output.
 */
class GraphEdgeTest {

  private static final GraphNode A = new GraphNode.External("db", Arrays.asList("a"));
  private static final GraphNode B = new GraphNode.External("db", Arrays.asList("b"));

  @Test
  void equalEdgesShareHashCode() {
    GraphEdge e1 = new GraphEdge(A, B, GraphEdge.Type.DEPENDS_ON_SOURCE);
    GraphEdge e2 = new GraphEdge(A, B, GraphEdge.Type.DEPENDS_ON_SOURCE);

    assertEquals(e1, e2);
    assertEquals(e1.hashCode(), e2.hashCode());
  }

  @Test
  void differentTypeMakesEdgesUnequal() {
    GraphEdge source = new GraphEdge(A, B, GraphEdge.Type.DEPENDS_ON_SOURCE);
    GraphEdge owner = new GraphEdge(A, B, GraphEdge.Type.OWNER_OF);
    assertNotEquals(source, owner,
        "an OWNER_OF and a DEPENDS_ON_SOURCE between the same nodes should coexist");
  }

  @Test
  void differentEndpointsMakeEdgesUnequal() {
    GraphEdge ab = new GraphEdge(A, B, GraphEdge.Type.TRIGGERS);
    GraphEdge ba = new GraphEdge(B, A, GraphEdge.Type.TRIGGERS);
    assertNotEquals(ab, ba, "edges are directed; A→B is distinct from B→A");
  }

  @Test
  void notEqualToOtherTypes() {
    GraphEdge e = new GraphEdge(A, B, GraphEdge.Type.TRIGGERS);
    assertNotEquals(e, "not an edge");
    assertNotEquals(e, null);
  }

  @Test
  void constructorRejectsNullArguments() {
    assertThrows(NullPointerException.class,
        () -> new GraphEdge(null, B, GraphEdge.Type.TRIGGERS));
    assertThrows(NullPointerException.class,
        () -> new GraphEdge(A, null, GraphEdge.Type.TRIGGERS));
    assertThrows(NullPointerException.class,
        () -> new GraphEdge(A, B, null));
  }

  @Test
  void toStringMentionsBothEndpointsAndType() {
    GraphEdge e = new GraphEdge(A, B, GraphEdge.Type.TRIGGERS);
    String s = e.toString();
    assertNotNull(s);
    assertTrue(s.contains(A.id()), "toString should include from id: " + s);
    assertTrue(s.contains(B.id()), "toString should include to id: " + s);
    assertTrue(s.contains("TRIGGERS"), "toString should include type: " + s);
  }
}
