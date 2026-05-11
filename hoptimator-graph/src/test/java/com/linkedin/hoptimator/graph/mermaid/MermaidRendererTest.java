package com.linkedin.hoptimator.graph.mermaid;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.GraphEdge;
import com.linkedin.hoptimator.GraphNode;
import com.linkedin.hoptimator.PipelineGraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Unit tests for {@link MermaidRenderer}. These exercise the rendering rules in isolation
 * (hand-built {@link PipelineGraph} fixtures, no K8s involvement).
 *
 * <p>Assertions check structural invariants — orientation, subgraph membership, edge styles,
 * driver-icon prefixes — rather than comparing the entire string verbatim. That keeps the tests
 * resilient to whitespace tweaks while pinning down the contracts that drive readability.
 */
class MermaidRendererTest {

  @Test
  void rendersMaterializedViewWithLrOrientation() {
    GraphNode.View root = new GraphNode.View("ns", "audience", true);
    GraphNode.External kafka = new GraphNode.External("kafka1",
        Arrays.asList("KAFKA", "events"), "kafka");
    GraphNode.External venice = new GraphNode.External("venice-prod",
        Arrays.asList("VENICE", "profile"), "venice");
    GraphNode.Pipeline pipeline = new GraphNode.Pipeline("ns", "audience", "INSERT INTO ...");
    GraphNode.External sink = new GraphNode.External("ads",
        Arrays.asList("VENICE", "audience"), "venice");

    Set<GraphNode> nodes = setOf(root, kafka, venice, pipeline, sink);
    Set<GraphEdge> edges = setOf(
        new GraphEdge(root, pipeline, GraphEdge.Type.OWNER_OF),
        new GraphEdge(kafka, pipeline, GraphEdge.Type.DEPENDS_ON_SOURCE),
        new GraphEdge(venice, pipeline, GraphEdge.Type.DEPENDS_ON_SOURCE),
        new GraphEdge(pipeline, sink, GraphEdge.Type.DEPENDS_ON_SINK));

    String mermaid = new MermaidRenderer().render(new PipelineGraph(root, nodes, edges));

    assertTrue(mermaid.startsWith("flowchart LR"),
        "Materialized view should render top-down LR: " + mermaid);
    assertTrue(mermaid.contains("⚡"), "kafka external should carry the driver icon prefix");
    assertTrue(mermaid.contains("📦"), "venice external should carry the driver icon prefix");
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
    GraphNode.LogicalTable root = new GraphNode.LogicalTable("ns", "foo", tiers);

    GraphNode.External nearline = new GraphNode.External("kafka-db",
        Arrays.asList("kafka-db", "foo"), "kafka");
    GraphNode.External online = new GraphNode.External("venice-db",
        Arrays.asList("venice-db", "foo"), "venice");
    GraphNode.External offline = new GraphNode.External("hdfs-db",
        Arrays.asList("hdfs-db", "foo"), "openhouse");
    GraphNode.Trigger trigger = new GraphNode.Trigger("ns", "foo-offline-trigger",
        "0 */6 * * *", false);

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
    assertTrue(mermaid.contains("cron: 0 */6 * * *"),
        "trigger label should expose the cron schedule: " + mermaid);
  }

  @Test
  void rendersTriggerPausedSuffix() {
    GraphNode.Trigger trigger = new GraphNode.Trigger("ns", "t1", "@hourly", true);
    GraphNode.External target = new GraphNode.External("db", Arrays.asList("path"), null);
    Set<GraphNode> nodes = setOf(trigger, target);
    Set<GraphEdge> edges = setOf(new GraphEdge(trigger, target, GraphEdge.Type.TRIGGERS));

    String mermaid = new MermaidRenderer().render(new PipelineGraph(trigger, nodes, edges));

    assertTrue(mermaid.contains("(paused)"),
        "paused trigger label should be marked: " + mermaid);
  }

  @Test
  void rendersJobKindAndEngineInsidePipelineLabel() {
    // Pipeline carries optional jobKind/engine pulled from V1alpha1PipelineSpec.yaml — those
    // surface inline in the Pipeline node label so the user sees the execution shape at a glance.
    GraphNode.Pipeline pipe = new GraphNode.Pipeline("ns", "p1", null,
        "FlinkDeployment", "Flink");
    Set<GraphNode> nodes = setOf(pipe);
    Set<GraphEdge> edges = setOf();

    String mermaid = new MermaidRenderer().render(new PipelineGraph(pipe, nodes, edges));

    assertTrue(mermaid.contains("kind: FlinkDeployment"),
        "pipeline label missing jobKind: " + mermaid);
    assertTrue(mermaid.contains("engine: Flink"),
        "pipeline label missing engine: " + mermaid);
  }

  @Test
  void externalWithoutDriverHasNoIcon() {
    GraphNode.External ext = new GraphNode.External("unknown-db", Arrays.asList("foo"), null);
    Set<GraphNode> nodes = setOf(ext);
    Set<GraphEdge> edges = setOf();

    String mermaid = new MermaidRenderer().render(new PipelineGraph(ext, nodes, edges));

    // No driver icon prefix at all when driver is unknown.
    assertTrue(!mermaid.contains("⚡"), "must not pick a default kafka icon: " + mermaid);
    assertTrue(!mermaid.contains("📦"), "must not pick a default venice icon: " + mermaid);
    assertTrue(mermaid.contains("foo"), "label should still include the path: " + mermaid);
  }

  @Test
  void emptyGraphRendersOrientationOnly() {
    GraphNode.View root = new GraphNode.View("ns", "v1", false);
    Set<GraphNode> nodes = setOf(root);
    Set<GraphEdge> edges = setOf();

    String mermaid = new MermaidRenderer().render(new PipelineGraph(root, nodes, edges));

    assertEquals("flowchart LR\n  n0[\"View v1\"]\n", mermaid);
  }

  @Test
  void rendersMultipleDistinctDriverIcons() {
    GraphNode.External k = new GraphNode.External("kafka1", Arrays.asList("topic"), "kafka");
    GraphNode.External v = new GraphNode.External("venice1", Arrays.asList("store"), "venice");
    GraphNode.External m = new GraphNode.External("mysql1", Arrays.asList("table"), "mysql");
    GraphNode.External o = new GraphNode.External("oh", Arrays.asList("ds"), "openhouse");
    GraphNode.View root = new GraphNode.View("ns", "v", true);

    Set<GraphNode> nodes = setOf(root, k, v, m, o);
    Set<GraphEdge> edges = setOf();

    String mermaid = new MermaidRenderer().render(new PipelineGraph(root, nodes, edges));

    assertTrue(mermaid.contains("⚡") && mermaid.contains("📦")
            && mermaid.contains("🗄") && mermaid.contains("📊"),
        "all four driver icons should appear in the rendered output: " + mermaid);
  }

  @Test
  void triggerWithoutScheduleOmitsCron() {
    // Triggers may be event-driven (no cron). Label should not show a "cron:" prefix in that case.
    GraphNode.Trigger trigger = new GraphNode.Trigger("ns", "t1", null, false);
    GraphNode.External target = new GraphNode.External("db", Arrays.asList("path"), null);
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
    GraphNode.View view = new GraphNode.View("ns", "audience", true);
    GraphNode.Pipeline pipeline = new GraphNode.Pipeline("ns", "audience-pipe", null);
    GraphNode.External sink = new GraphNode.External("ads", Arrays.asList("realized"), "venice");

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
    GraphNode.View view = new GraphNode.View("ns", "audience", true);
    GraphNode.Pipeline pipeline = new GraphNode.Pipeline("ns", "audience-pipe", null);
    GraphNode.External sink = new GraphNode.External("ads", Arrays.asList("realized"), "venice");

    Set<GraphNode> nodes = setOf(view, pipeline, sink);
    Set<GraphEdge> edges = setOf(
        new GraphEdge(view, pipeline, GraphEdge.Type.OWNER_OF),
        new GraphEdge(pipeline, sink, GraphEdge.Type.DEPENDS_ON_SINK));

    String mermaid = new MermaidRenderer().render(new PipelineGraph(view, nodes, edges));

    assertTrue(mermaid.contains("subgraph "),
        "View owning a pipeline should produce a subgraph wrapper: " + mermaid);
    assertTrue(mermaid.contains("Materialized View audience"),
        "View display name should be the subgraph label: " + mermaid);
  }

  @Test
  void multipleSourcesAllConnectToTheSamePipeline() {
    GraphNode.View root = new GraphNode.View("ns", "fanin", true);
    GraphNode.Pipeline pipe = new GraphNode.Pipeline("ns", "fanin-pipe", null);
    GraphNode.External a = new GraphNode.External("db1", Arrays.asList("a"), "kafka");
    GraphNode.External b = new GraphNode.External("db2", Arrays.asList("b"), "kafka");
    GraphNode.External c = new GraphNode.External("db3", Arrays.asList("c"), "venice");

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
