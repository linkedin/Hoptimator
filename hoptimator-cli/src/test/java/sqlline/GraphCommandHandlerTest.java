package sqlline;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.PipelineGraph;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Unit tests for the static argument-parsing helpers on {@link HoptimatorAppConfig.GraphCommandHandler}.
 *
 * <p>Exercising the full {@code execute(...)} path is impractical without spinning up a sqlline
 * shell, so the assertions here focus on the bits of logic that are most likely to regress.
 */
class GraphCommandHandlerTest {

  @Test
  void isDegenerateTrueForRootOnlyGraph() {
    GraphNode.External root = new GraphNode.External("db", List.of("table"));
    Set<GraphNode> nodes = singleton(root);
    Set<GraphEdge> edges = empty();

    PipelineGraph graph = new PipelineGraph(root, nodes, edges);

    assertTrue(HoptimatorAppConfig.GraphCommandHandler.isDegenerate(graph),
        "single-node, zero-edge graph is degenerate");
  }

  @Test
  void isDegenerateFalseWhenEdgesExist() {
    GraphNode.External root = new GraphNode.External("db", List.of("a"));
    GraphNode.Pipeline pipe = new GraphNode.Pipeline("p", null, null, null);
    Set<GraphNode> nodes = pair(root, pipe);
    Set<GraphEdge> edges = oneEdge(new GraphEdge(root, pipe, GraphEdge.Type.DEPENDS_ON_SOURCE));

    PipelineGraph graph = new PipelineGraph(root, nodes, edges);

    assertFalse(HoptimatorAppConfig.GraphCommandHandler.isDegenerate(graph),
        "graph with at least one edge is not degenerate");
  }

  @Test
  void degenerateGraphWarningStartsWithMermaidCommentSyntax() {
    String warning = HoptimatorAppConfig.GraphCommandHandler.degenerateGraphWarning();

    assertTrue(warning.startsWith("%% "),
        "warning must use Mermaid comment syntax so renderers ignore it: " + warning);
    assertTrue(warning.toLowerCase().contains("warning"),
        "warning text should be self-evident: " + warning);
  }

  private static Set<GraphNode> singleton(GraphNode n) {
    Set<GraphNode> set = new LinkedHashSet<>();
    set.add(n);
    return set;
  }

  private static Set<GraphNode> pair(GraphNode a, GraphNode b) {
    Set<GraphNode> set = new LinkedHashSet<>();
    set.add(a);
    set.add(b);
    return set;
  }

  private static Set<GraphEdge> empty() {
    return new LinkedHashSet<>();
  }

  private static Set<GraphEdge> oneEdge(GraphEdge edge) {
    Set<GraphEdge> set = new LinkedHashSet<>();
    set.add(edge);
    return set;
  }
}
