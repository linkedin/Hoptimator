package com.linkedin.hoptimator.graph.mermaid;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.PipelineGraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Unit tests for {@link MermaidRenderer}. These exercise the rendering rules in isolation
 * (hand-built {@link PipelineGraph} fixtures, no K8s involvement).
 *
 * <p>Assertions check structural invariants — orientation, subgraph membership, edge styles —
 * rather than comparing the entire string verbatim. That keeps the tests resilient to whitespace
 * tweaks while pinning down the contracts that drive readability.
 */
class MermaidRendererTest {

  @Test
  void rendersMaterializedViewWithLrOrientation() {
    GraphNode.View root = new GraphNode.View("audience", true);
    GraphNode.External kafka = new GraphNode.External("kafka1",
        Arrays.asList("KAFKA", "events"));
    GraphNode.External venice = new GraphNode.External("venice-prod",
        Arrays.asList("VENICE", "profile"));
    GraphNode.Pipeline pipeline = new GraphNode.Pipeline("audience", null, null);
    GraphNode.External sink = new GraphNode.External("ads",
        Arrays.asList("VENICE", "audience"));

    Set<GraphNode> nodes = setOf(root, kafka, venice, pipeline, sink);
    Set<GraphEdge> edges = setOf(
        new GraphEdge(root, pipeline, GraphEdge.Type.OWNER_OF),
        new GraphEdge(kafka, pipeline, GraphEdge.Type.DEPENDS_ON_SOURCE),
        new GraphEdge(venice, pipeline, GraphEdge.Type.DEPENDS_ON_SOURCE),
        new GraphEdge(pipeline, sink, GraphEdge.Type.DEPENDS_ON_SINK));

    String mermaid = new MermaidRenderer().render(new PipelineGraph(root, nodes, edges));

    assertTrue(mermaid.startsWith("flowchart LR"),
        "Materialized view should render top-down LR: " + mermaid);
    // OWNER_OF must drive the subgraph wrapper, not an arrow.
    assertTrue(!mermaid.contains("--OWNER_OF"), "OWNER_OF must not surface as an arrow");
    // The view's owned pipeline lives inside the View's subgraph wrapper.
    assertTrue(mermaid.contains("subgraph "), "owners get subgraph wrappers");
  }

  @Test
  void rendersLogicalTableTopDownWithTierSubgraphs() {
    Map<String, String> tiers = new LinkedHashMap<>();
    tiers.put("nearline", "kafka-db");
    tiers.put("online", "venice-db");
    tiers.put("offline", "hdfs-db");
    GraphNode.LogicalTable root = new GraphNode.LogicalTable("foo", tiers);

    GraphNode.External nearline = new GraphNode.External("kafka-db",
        Arrays.asList("kafka-db", "foo"));
    GraphNode.External online = new GraphNode.External("venice-db",
        Arrays.asList("venice-db", "foo"));
    GraphNode.External offline = new GraphNode.External("hdfs-db",
        Arrays.asList("hdfs-db", "foo"));
    GraphNode.Trigger trigger = new GraphNode.Trigger("foo-offline-trigger",
        "0 */6 * * *", false, null, null);

    Set<GraphNode> nodes = setOf(root, nearline, online, offline, trigger);
    Set<GraphEdge> edges = setOf(
        new GraphEdge(root, nearline, GraphEdge.Type.OWNER_OF),
        new GraphEdge(root, online, GraphEdge.Type.OWNER_OF),
        new GraphEdge(root, offline, GraphEdge.Type.OWNER_OF),
        new GraphEdge(root, trigger, GraphEdge.Type.OWNER_OF),
        new GraphEdge(trigger, offline, GraphEdge.Type.TRIGGERS));

    String mermaid = new MermaidRenderer().render(new PipelineGraph(root, nodes, edges));

    assertTrue(mermaid.startsWith("flowchart TD"),
        "LogicalTable should render TD outer, LR inner: " + mermaid);
    assertTrue(mermaid.contains("direction LR"),
        "LogicalTable subgraph should set inner direction LR: " + mermaid);
    // Each tier should produce its own nested subgraph header.
    assertTrue(mermaid.contains("nearline"), "nearline tier subgraph missing: " + mermaid);
    assertTrue(mermaid.contains("online"), "online tier subgraph missing: " + mermaid);
    assertTrue(mermaid.contains("offline"), "offline tier subgraph missing: " + mermaid);
    // Trigger arrow uses dotted edge.
    assertTrue(mermaid.contains(" -.-> "), "trigger edge should be dotted: " + mermaid);
    // Don't pin the exact phrasing — that's cron-utils' contract. Just verify the schedule
    // makes it onto the trigger label in humanized form rather than the raw cron string.
    assertTrue(mermaid.contains("cron: ") && mermaid.contains("6") && mermaid.contains("hour"),
        "trigger label should expose the cron schedule humanized: " + mermaid);
  }

  @Test
  void rendersTriggerPausedSuffix() {
    GraphNode.Trigger trigger = new GraphNode.Trigger("t1", "@hourly", true, null, null);
    GraphNode.External target = new GraphNode.External("db", Arrays.asList("path"));
    Set<GraphNode> nodes = setOf(trigger, target);
    Set<GraphEdge> edges = setOf(new GraphEdge(trigger, target, GraphEdge.Type.TRIGGERS));

    String mermaid = new MermaidRenderer().render(new PipelineGraph(trigger, nodes, edges));

    assertTrue(mermaid.contains("(paused)"),
        "paused trigger label should be marked: " + mermaid);
  }

  @Test
  void rendersJobKindAndEngineInsidePipelineLabel() {
    // Pipeline carries optional jobKind/engine pulled from the underlying job artifact — those
    // surface inline in the Pipeline node label so the user sees the execution shape at a glance.
    GraphNode.Pipeline pipe = new GraphNode.Pipeline("p1", "FlinkDeployment", "Flink");
    Set<GraphNode> nodes = setOf(pipe);
    Set<GraphEdge> edges = setOf();

    String mermaid = new MermaidRenderer().render(new PipelineGraph(pipe, nodes, edges));

    assertTrue(mermaid.contains("kind: FlinkDeployment"),
        "pipeline label missing jobKind: " + mermaid);
    assertTrue(mermaid.contains("engine: Flink"),
        "pipeline label missing engine: " + mermaid);
  }

  @Test
  void emptyGraphRendersOrientationOnly() {
    GraphNode.View root = new GraphNode.View("v1", false);
    Set<GraphNode> nodes = setOf(root);
    Set<GraphEdge> edges = setOf();

    String mermaid = new MermaidRenderer().render(new PipelineGraph(root, nodes, edges));

    // Leaf View renders with just the name — rectangle shape conveys "view-ness".
    assertEquals("flowchart LR\n  n0[\"v1\"]\n", mermaid);
  }

  @Test
  void triggerWithoutScheduleOmitsCron() {
    // Triggers may be event-driven (no cron). Label should not show a "cron:" prefix in that case.
    GraphNode.Trigger trigger = new GraphNode.Trigger("t1", null, false, null, null);
    GraphNode.External target = new GraphNode.External("db", Arrays.asList("path"));
    Set<GraphNode> nodes = setOf(trigger, target);
    Set<GraphEdge> edges = setOf(new GraphEdge(trigger, target, GraphEdge.Type.TRIGGERS));

    String mermaid = new MermaidRenderer().render(new PipelineGraph(trigger, nodes, edges));

    assertTrue(!mermaid.contains("cron:"), "trigger without schedule should omit cron line: " + mermaid);
  }

  @Test
  void ownerSubgraphDoesNotEmitOwnerAsItsOwnChildNode() {
    // Mermaid rejects "Setting n0 as parent of n0 would create a cycle" when a subgraph id
    // collides with a node id inside it. Pin down the invariant: when an owner gets a subgraph
    // wrapper, its mermaid id must appear only as the subgraph header — never as a node line.
    GraphNode.View view = new GraphNode.View("audience", true);
    GraphNode.Pipeline pipeline = new GraphNode.Pipeline("audience-pipe", null, null);
    GraphNode.External sink = new GraphNode.External("ads", Arrays.asList("realized"));

    Set<GraphNode> nodes = setOf(view, pipeline, sink);
    Set<GraphEdge> edges = setOf(
        new GraphEdge(view, pipeline, GraphEdge.Type.OWNER_OF),
        new GraphEdge(pipeline, sink, GraphEdge.Type.DEPENDS_ON_SINK));

    String mermaid = new MermaidRenderer().render(new PipelineGraph(view, nodes, edges));

    // n0 should appear once on the subgraph header line and never as a `n0[...]` node line.
    int subgraphHeaders = countOccurrences(mermaid, "subgraph n0[");
    int nodeLines = countOccurrences(mermaid, "  n0[\"");
    assertEquals(1, subgraphHeaders, "owner should be the subgraph header exactly once: " + mermaid);
    assertEquals(0, nodeLines, "owner must not also appear as a node line inside its own subgraph: " + mermaid);
  }

  @Test
  void ownerSubgraphWrapsOwnedChildrenForNonLogicalTable() {
    // When a non-LogicalTable node owns children (e.g. a View owning its realizing pipeline),
    // the owner gets a subgraph wrapper with the children inside.
    GraphNode.View view = new GraphNode.View("audience", true);
    GraphNode.Pipeline pipeline = new GraphNode.Pipeline("audience-pipe", null, null);
    GraphNode.External sink = new GraphNode.External("ads", Arrays.asList("realized"));

    Set<GraphNode> nodes = setOf(view, pipeline, sink);
    Set<GraphEdge> edges = setOf(
        new GraphEdge(view, pipeline, GraphEdge.Type.OWNER_OF),
        new GraphEdge(pipeline, sink, GraphEdge.Type.DEPENDS_ON_SINK));

    String mermaid = new MermaidRenderer().render(new PipelineGraph(view, nodes, edges));

    assertTrue(mermaid.contains("subgraph "),
        "View owning a pipeline should produce a subgraph wrapper: " + mermaid);
    // Subgraph wrapper carries only the kind ("Materialized View") — the inner pipeline node
    // already shows the resource name, so repeating it on the wrapper is just noise.
    assertTrue(mermaid.contains("[\"Materialized View\"]"),
        "View subgraph label should be just the kind without the name: " + mermaid);
    assertTrue(mermaid.contains("audience"),
        "the name should still appear (on the inner pipeline node): " + mermaid);
  }

  @Test
  void multipleSourcesAllConnectToTheSamePipeline() {
    GraphNode.View root = new GraphNode.View("fanin", true);
    GraphNode.Pipeline pipe = new GraphNode.Pipeline("fanin-pipe", null, null);
    GraphNode.External a = new GraphNode.External("db1", Arrays.asList("a"));
    GraphNode.External b = new GraphNode.External("db2", Arrays.asList("b"));
    GraphNode.External c = new GraphNode.External("db3", Arrays.asList("c"));

    Set<GraphNode> nodes = setOf(root, pipe, a, b, c);
    Set<GraphEdge> edges = setOf(
        new GraphEdge(root, pipe, GraphEdge.Type.OWNER_OF),
        new GraphEdge(a, pipe, GraphEdge.Type.DEPENDS_ON_SOURCE),
        new GraphEdge(b, pipe, GraphEdge.Type.DEPENDS_ON_SOURCE),
        new GraphEdge(c, pipe, GraphEdge.Type.DEPENDS_ON_SOURCE));

    String mermaid = new MermaidRenderer().render(new PipelineGraph(root, nodes, edges));

    int arrowCount = countOccurrences(mermaid, " --> ");
    assertEquals(3, arrowCount, "three source arrows expected: " + mermaid);
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    int idx = 0;
    while ((idx = haystack.indexOf(needle, idx)) != -1) {
      count++;
      idx += needle.length();
    }
    return count;
  }

  @SafeVarargs
  private static <T> Set<T> setOf(T... items) {
    Set<T> set = new LinkedHashSet<>();
    for (T item : items) {
      set.add(item);
    }
    return set;
  }
}
