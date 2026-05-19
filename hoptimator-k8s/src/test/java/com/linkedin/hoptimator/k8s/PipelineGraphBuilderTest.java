package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.PipelineGraph;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewSpec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * End-to-end tests for {@link PipelineGraphBuilder}. Each test seeds a small fixture of CRDs
 * via {@link FakeK8sApi} and exercises one of the three entry points. Assertions probe graph
 * structure (node kinds, edge types, ownership wiring) rather than rendered output, which is
 * tested separately in {@code MermaidRendererTest}.
 */
class PipelineGraphBuilderTest {

  // ─── Test 1: simple MV with two K8s sources + external sink ──────────────

  @Test
  void forViewMaterializedRendersSourcesAndSink() throws SQLException {
    V1alpha1View view = view("ads", "audience", true, "view-uid-1");
    V1alpha1Pipeline pipeline = pipelineWithSourcesAndSink(
        "audience-pipeline", "view-uid-1", "View",
        Arrays.asList("kafka1_KAFKA.events", "venice-prod_VENICE.profile"),
        "ads_VENICE.audience");

    PipelineGraphBuilder builder = builder(
        list(view), list(pipeline), list(), list());

    PipelineGraph graph = builder.forView("audience", 1);

    assertEquals(GraphNode.Kind.VIEW, graph.root().kind());
    // Expected nodes: View root + 1 Pipeline + 2 sources + 1 sink = 5 nodes.
    assertEquals(5, graph.nodes().size(), "graph should have view + pipeline + 2 sources + sink");

    GraphNode pipelineNode = graphNodeByKind(graph, GraphNode.Kind.PIPELINE);
    assertNotNull(pipelineNode);
    assertTrue(graph.edges().stream().anyMatch(e ->
            e.type() == GraphEdge.Type.OWNER_OF
                && e.from().equals(graph.root())
                && e.to().equals(pipelineNode)),
        "View should own the realizing pipeline");

    long sourceEdges = graph.edges().stream()
        .filter(e -> e.type() == GraphEdge.Type.DEPENDS_ON_SOURCE).count();
    assertEquals(2, sourceEdges, "two source edges expected");

    long sinkEdges = graph.edges().stream()
        .filter(e -> e.type() == GraphEdge.Type.DEPENDS_ON_SINK).count();
    assertEquals(1, sinkEdges, "one sink edge expected");
  }

  // ─── Test 2: MV-on-MV — Calcite inlining, no special-case handling ────────

  @Test
  void forViewMvOnMvRendersOnlyPhysicalSources() throws SQLException {
    // CREATE MV A AS SELECT ... FROM kafka.x
    // CREATE MV B AS SELECT * FROM A
    // After Calcite inlining, B's pipeline reads kafka.x directly. The graph for B should NOT
    // mention A — visualization tracks operational truth, not user intent.
    V1alpha1View viewB = view("ns", "b", true, "uid-B");
    V1alpha1Pipeline pipeB = pipelineWithSourcesAndSink(
        "B-pipeline", "uid-B", "View",
        Arrays.asList("kafka1_KAFKA.x"),
        "ns_VENICE.B");

    PipelineGraphBuilder builder = builder(
        list(view("ns", "a", true, "uid-A"), viewB),
        list(pipeB),
        list(), list());

    PipelineGraph graph = builder.forView("b", 1);

    boolean mentionsA = graph.nodes().stream().anyMatch(n -> n.id().endsWith("/a"));
    assertFalse(mentionsA, "MV-on-MV should not surface upstream View 'A' — only physical sources");
    assertTrue(graph.nodes().stream().anyMatch(n ->
            n instanceof GraphNode.External && ((GraphNode.External) n).database().equals("kafka1")),
        "MV-on-MV should surface inlined kafka source");
  }

  // ─── Test 2b: forView is scoped to the view's neighborhood ────────────────

  @Test
  void forViewIsScopedToTheViewNeighborhood() throws SQLException {
    // Mirrors the real-world bug. Two MVs share a table as their connection point:
    //   MV "P" reads from ads.ad_clicks and writes to ads.page_views.
    //   MV "A" reads from ads.page_views (and profile.members) and writes to ads.audience.
    // forView on A should show A's own pipeline + sources + sink, nothing else:
    //   - No P-pipeline (upstream — chain belongs to Resource targets via !graph on the source).
    //   - No ads.ad_clicks (P's source, not A's direct neighbor).
    // forView on P should show P's own pipeline + sources + sink, nothing else:
    //   - No A-pipeline (downstream of P's sink).
    //   - No profile.members / ads.audience.
    V1alpha1View viewP = view("ads", "p", true, "uid-P");
    V1alpha1Pipeline pPipe = pipelineWithSourcesAndSink(
        "p-pipeline", "uid-P", "View",
        Arrays.asList("ads-database_ADS.AD_CLICKS"),
        "ads-database_ADS.PAGE_VIEWS");

    V1alpha1View viewA = view("ads", "a", true, "uid-A");
    V1alpha1Pipeline aPipe = pipelineWithSourcesAndSink(
        "a-pipeline", "uid-A", "View",
        Arrays.asList("ads-database_ADS.PAGE_VIEWS", "profile-database_PROFILE.MEMBERS"),
        "ads-database_ADS.AUDIENCE");

    PipelineGraphBuilder builder = builder(
        list(viewP, viewA),
        list(pPipe, aPipe),
        list(), list());

    // forView P — must NOT include downstream A or its surroundings.
    PipelineGraph forP = builder.forView("p", 2);
    boolean pHasOwn = forP.nodes().stream().anyMatch(n ->
        n instanceof GraphNode.Pipeline && "p-pipeline".equals(((GraphNode.Pipeline) n).name()));
    boolean pHasA = forP.nodes().stream().anyMatch(n ->
        n instanceof GraphNode.Pipeline && "a-pipeline".equals(((GraphNode.Pipeline) n).name()));
    assertTrue(pHasOwn, "P's own pipeline should appear in its graph");
    assertFalse(pHasA, "A's pipeline is downstream of P's sink — must not appear");
    assertFalse(forP.nodes().stream().anyMatch(n ->
            n instanceof GraphNode.External
                && "profile-database".equals(((GraphNode.External) n).database())),
        "profile.members only feeds the downstream MV — must not appear");

    // forView A — must NOT include upstream P or ads.ad_clicks. The chain view is the job of
    // a Resource-target query (!graph on the source identifier). A's graph is A's pipeline +
    // its direct sources (PAGE_VIEWS, MEMBERS as leaves) + its sink (AUDIENCE as a leaf).
    PipelineGraph forA = builder.forView("a", 2);
    boolean aHasOwn = forA.nodes().stream().anyMatch(n ->
        n instanceof GraphNode.Pipeline && "a-pipeline".equals(((GraphNode.Pipeline) n).name()));
    boolean aHasP = forA.nodes().stream().anyMatch(n ->
        n instanceof GraphNode.Pipeline && "p-pipeline".equals(((GraphNode.Pipeline) n).name()));
    assertTrue(aHasOwn, "A's own pipeline should appear in its graph");
    assertFalse(aHasP, "P's pipeline is upstream of A's source — must not chain into A's view graph");
    assertFalse(forA.nodes().stream().anyMatch(n ->
            n instanceof GraphNode.External
                && ((GraphNode.External) n).path().contains("AD_CLICKS")),
        "ads.ad_clicks is upstream of A's source PAGE_VIEWS — must not chain into A's view graph");
  }

  // ─── Test 3: LogicalTable with three tiers + offline trigger ─────────────

  @Test
  void forLogicalTableRendersTiersAndOwnedPipelinesAndTrigger() throws SQLException {
    V1alpha1LogicalTable lt = logicalTable("ns", "events", "uid-lt", linked(
        "nearline", "kafka-db",
        "online", "venice-db",
        "offline", "hdfs-db"));

    // Implicit nearline → online pipeline owned by the LogicalTable.
    V1alpha1Pipeline n2o = pipelineWithSourcesAndSink(
        "events-nearline-online", "uid-lt", "LogicalTable",
        Arrays.asList("kafka-db_kafka-db.events"),
        "venice-db_venice-db.events");
    V1alpha1Pipeline n2f = pipelineWithSourcesAndSink(
        "events-nearline-offline", "uid-lt", "LogicalTable",
        Arrays.asList("kafka-db_kafka-db.events"),
        "hdfs-db_hdfs-db.events");
    // Bridging trigger: reads from offline (source) and writes to online (sink), reverse-ETL.
    V1alpha1TableTrigger trigger = trigger("ns", "events-offline-trigger",
        "uid-lt", "0 */6 * * *",
        "hdfs-db_hdfs-db.events",      // source (offline)
        "venice-db_venice-db.events"); // sink (online)

    PipelineGraphBuilder builder = builder(
        list(),
        list(n2o, n2f),
        list(lt),
        list(trigger));

    PipelineGraph graph = builder.forLogicalTable("events", 1);

    assertEquals(GraphNode.Kind.LOGICAL_TABLE, graph.root().kind());
    long pipelineCount = graph.nodes().stream()
        .filter(n -> n.kind() == GraphNode.Kind.PIPELINE).count();
    assertEquals(2, pipelineCount, "two implicit pipelines expected");

    long triggerCount = graph.nodes().stream()
        .filter(n -> n.kind() == GraphNode.Kind.TRIGGER).count();
    assertEquals(1, triggerCount, "offline trigger expected");

    long triggerEdges = graph.edges().stream()
        .filter(e -> e.type() == GraphEdge.Type.TRIGGERS).count();
    assertEquals(2, triggerEdges,
        "bridging trigger should produce two TRIGGERS edges: source→trigger and trigger→sink");

    // LT now owns whatever externals match a tier database, plus the 2 pipelines + 1 trigger.
    // Three tier-physical externals come from the pipelines (one per tier db) plus the 2
    // pipelines + 1 trigger = 6 OWNER_OF edges.
    long ownerEdges = graph.edges().stream()
        .filter(e -> e.type() == GraphEdge.Type.OWNER_OF
            && e.from().equals(graph.root())).count();
    assertEquals(6, ownerEdges,
        "LogicalTable should own its tier externals + 2 pipelines + 1 trigger; got: " + ownerEdges);
  }

  // ─── Test: multiple triggers on a logical table — both are surfaced ──────

  @Test
  void forLogicalTableSurfacesMultipleTriggers() throws SQLException {
    V1alpha1LogicalTable lt = logicalTable("ns", "events", "uid-lt", linked(
        "nearline", "kafka-db",
        "offline", "hdfs-db"));

    // Both triggers reference the same offline-tier identifier so the graph connects both.
    // We add a pipeline writing to that identifier so the External actually exists in the graph
    // (otherwise the trigger has nothing to connect to).
    V1alpha1Pipeline writer = pipelineWithSourcesAndSink(
        "events-nearline-offline", "uid-lt", "LogicalTable",
        Arrays.asList("kafka-db_kafka-db.events"),
        "hdfs-db_hdfs-db.events");
    V1alpha1TableTrigger fast = trigger("ns", "events-fast", "uid-lt",
        "*/5 * * * *", "hdfs-db_hdfs-db.events", null);
    V1alpha1TableTrigger slow = trigger("ns", "events-slow", "uid-lt",
        "0 0 * * *", "hdfs-db_hdfs-db.events", null);

    PipelineGraphBuilder builder = builder(
        list(), list(writer), list(lt), list(fast, slow));

    PipelineGraph graph = builder.forLogicalTable("events", 0);

    long triggerNodes = graph.nodes().stream()
        .filter(n -> n.kind() == GraphNode.Kind.TRIGGER).count();
    assertEquals(2, triggerNodes, "both triggers should be in the graph");

    long triggerEdges = graph.edges().stream()
        .filter(e -> e.type() == GraphEdge.Type.TRIGGERS).count();
    assertEquals(2, triggerEdges, "both TRIGGERS edges should exist");
  }

  // ─── Test: trigger pointing at an arbitrary table (not LogicalTable-owned) ─

  @Test
  void forResourceTraversesIntoTriggerTarget() throws SQLException {
    // A standalone trigger that fires a pipeline writing to an arbitrary external table.
    // forResource on that table should still surface the connected pipeline; the trigger itself
    // doesn't live in this graph (it's not owned by the resource), but the pipeline does.
    V1alpha1Pipeline pipeline = pipelineWithSourcesAndSink(
        "etl-pipeline", "uid-job", "Job",
        Arrays.asList("source-db_S.input"),
        "sink-db_S.output");

    PipelineGraphBuilder builder = builder(
        list(), list(pipeline), list(), list());

    PipelineGraph graph = builder.forResource("sink-db", Arrays.asList("S", "output"), 1);

    long pipelineCount = graph.nodes().stream()
        .filter(n -> n.kind() == GraphNode.Kind.PIPELINE).count();
    assertEquals(1, pipelineCount, "reverse lookup should surface the writer pipeline");

    boolean hasSinkEdge = graph.edges().stream()
        .anyMatch(e -> e.type() == GraphEdge.Type.DEPENDS_ON_SINK);
    assertTrue(hasSinkEdge, "writer pipeline should connect to sink via DEPENDS_ON_SINK");
  }

  // ─── Test: depth=0 stops at the root only ─────────────────────────────────

  @Test
  void depthZeroForResourceReturnsRootOnly() throws SQLException {
    V1alpha1Pipeline pipeline = pipelineWithSourcesAndSink(
        "p1", "uid", "Job",
        Arrays.asList("kafka1_KAFKA.events"),
        "ads_VENICE.audience");

    PipelineGraphBuilder builder = builder(
        list(), list(pipeline), list(), list());

    PipelineGraph graph = builder.forResource("kafka1", Arrays.asList("KAFKA", "events"), 0);

    assertEquals(1, graph.nodes().size(),
        "depth=0 should yield only the root resource");
  }

  // ─── Test: cycles in the pipeline graph terminate cleanly ────────────────

  @Test
  void forResourceTerminatesOnCycle() throws SQLException {
    // Cycle: P1 reads T1 / writes T2; P2 reads T2 / writes T1. Same resource appears as both
    // source and sink along the chain, which would loop forever without the Traversal's
    // visited-set guard. Pin that the build still terminates and the rendered graph captures
    // the loop as a closed cycle (4 nodes, 4 edges).
    V1alpha1Pipeline p1 = pipelineWithSourcesAndSink(
        "p1", "uid-job-1", "Job",
        Arrays.asList("db_A.t1"),
        "db_A.t2");
    V1alpha1Pipeline p2 = pipelineWithSourcesAndSink(
        "p2", "uid-job-2", "Job",
        Arrays.asList("db_A.t2"),
        "db_A.t1");

    PipelineGraphBuilder builder = builder(
        list(), list(p1, p2), list(), list());

    // Generous depth to make sure termination comes from the visited-set, not the cap.
    PipelineGraph graph = builder.forResource("db", Arrays.asList("A", "t1"), 10);

    // Exactly 4 nodes: T1 (root), P1, T2, P2 — no duplicate node identities.
    assertEquals(4, graph.nodes().size(),
        "cycle should produce 4 distinct nodes (T1, P1, T2, P2), not infinite expansion");

    // Exactly 4 edges in the loop:
    //   T1 → P1 (DEPENDS_ON_SOURCE)
    //   P1 → T2 (DEPENDS_ON_SINK)
    //   T2 → P2 (DEPENDS_ON_SOURCE)
    //   P2 → T1 (DEPENDS_ON_SINK)
    assertEquals(4, graph.edges().size(),
        "cycle should produce 4 distinct edges, no duplicates from re-expansion");

    // The second arc closes the loop — P2's sink points back at the root.
    boolean cycleCloses = graph.edges().stream()
        .anyMatch(e -> e.type() == GraphEdge.Type.DEPENDS_ON_SINK
            && e.to().equals(graph.root()));
    assertTrue(cycleCloses, "an edge should point back at the root to close the cycle");
  }

  // ─── Test: large depth values are accepted ────────────────────────────────

  @Test
  void largeDepthHandledGracefully() throws SQLException {
    V1alpha1Pipeline pipeline = pipelineWithSourcesAndSink(
        "p1", "uid", "Job",
        Arrays.asList("kafka1_KAFKA.events"),
        "ads_VENICE.audience");

    PipelineGraphBuilder builder = builder(
        list(), list(pipeline), list(), list());

    // Depth 999: in practice graphs don't get that deep, so recursion terminates anyway.
    PipelineGraph graph = builder.forResource("kafka1", Arrays.asList("KAFKA", "events"), 999);

    assertNotNull(graph);
    assertTrue(graph.nodes().size() >= 2, "large depth should still discover at least the pipeline");
  }

  // ─── Test: forView accepts SQL-style identifiers and canonicalizes them ──

  @Test
  void forViewCanonicalizesSqlStyleIdentifier() throws SQLException {
    // CRD is stored as `venice-test-store-insert-partial`. The user passes the SQL-side identifier
    // they typed in their CREATE statement, which has uppercase, dots, and a `$`.
    V1alpha1View view = view("ns", "venice-test-store-insert-partial", true, "uid-v");

    PipelineGraphBuilder builder = builder(
        list(view), list(), list(), list());

    PipelineGraph graph = builder.forView("VENICE.test-store$insert-partial", 0);

    assertEquals(GraphNode.Kind.VIEW, graph.root().kind());
    assertEquals("venice-test-store-insert-partial", ((GraphNode.View) graph.root()).name(),
        "root name should be the canonicalized form");
  }

  // ─── Test: extractJobKind matches only kinds ending in "Job" ─────────────

  @Test
  void extractJobKindReturnsTheJobSuffixedKind() {
    String yaml = ""
        + "apiVersion: hoptimator.linkedin.com/v1alpha1\n"
        + "kind: KafkaTopic\n"
        + "metadata:\n  name: foo\n"
        + "---\n"
        + "apiVersion: flink.apache.org/v1beta1\n"
        + "kind: FlinkSessionJob\n"
        + "metadata:\n  name: foo-job\n";

    assertEquals("FlinkSessionJob", PipelineGraphBuilder.extractJobKind(yaml),
        "should pick the kind whose name ends in 'Job', not any sibling resource");
  }

  @Test
  void extractJobKindReturnsNullWhenNoJobSuffixedKindPresent() {
    String yaml = "kind: KafkaTopic\n---\nkind: VeniceStore\n---\nkind: ConfigMap\n";

    assertNull(PipelineGraphBuilder.extractJobKind(yaml),
        "no Job-suffixed kind → null (we don't fabricate a job)");
  }

  @Test
  void inferEngineNullJobKindReturnsNull() {
    assertNull(PipelineGraphBuilder.inferEngine(null, "yaml content"));
  }

  @Test
  void inferEngineSparkJobReturnsSpark() {
    assertEquals("Spark", PipelineGraphBuilder.inferEngine("SparkApplication", null));
  }

  @Test
  void inferEngineBeamOnFlinkReturnsFlinkBeam() {
    // jobKind hints at Flink, yaml mentions Beam — engine is Flink Beam (Beam pipelines running
    // on a Flink runner).
    assertEquals("Flink Beam", PipelineGraphBuilder.inferEngine("FlinkSessionJob",
        "containers:\n  - name: beam-worker\n"));
  }

  @Test
  void inferEngineBeamOnlyJobReturnsBeam() {
    // Beam without a Flink runner declared — fall through to plain Beam.
    assertEquals("Beam", PipelineGraphBuilder.inferEngine("BeamApplication", null));
  }

  @Test
  void inferEngineUnknownEngineReturnsNull() {
    // No flink/beam/spark keyword anywhere — caller renders without an engine line.
    assertNull(PipelineGraphBuilder.inferEngine("CustomKind", null));
  }

  @Test
  void extractJobKindHandlesPlainBatchJob() {
    String yaml = ""
        + "apiVersion: batch/v1\n"
        + "kind: Job\n"
        + "metadata:\n  name: cron-job\n";

    assertEquals("Job", PipelineGraphBuilder.extractJobKind(yaml));
  }

  @Test
  void extractJobTemplateNameStripsTriggerPrefix() {
    // Deployer renders `metadata.name: <trigger>-<template>`. Strip the prefix to recover
    // the template name (which the visualizer uses to label trigger nodes).
    String yaml = ""
        + "apiVersion: batch/v1\n"
        + "kind: Job\n"
        + "metadata:\n"
        + "  name: testsimple-my-app\n"
        + "  namespace: my-mp\n";

    assertEquals("my-app", PipelineGraphBuilder.extractJobTemplateName("testsimple", yaml));
  }

  @Test
  void extractJobTemplateNameReturnsNullWhenPrefixMissing() {
    // YAML doesn't follow `<trigger>-<template>` — bail rather than misattribute.
    String yaml = "metadata:\n  name: someother-job\n";
    assertNull(PipelineGraphBuilder.extractJobTemplateName("testsimple", yaml));
  }

  @Test
  void extractJobTemplateNameReturnsNullWhenNoMetadataName() {
    assertNull(PipelineGraphBuilder.extractJobTemplateName("testsimple", "no metadata here"));
    assertNull(PipelineGraphBuilder.extractJobTemplateName("testsimple", ""));
    assertNull(PipelineGraphBuilder.extractJobTemplateName("testsimple", null));
  }

  @Test
  void extractMetadataNameSkipsContainerName() {
    // `name:` also appears under containers — must not match a container entry. The metadata
    // line is indented but has no leading `-`, which is how we tell them apart.
    String yaml = ""
        + "metadata:\n"
        + "  name: outer-name\n"
        + "spec:\n"
        + "  template:\n"
        + "    spec:\n"
        + "      containers:\n"
        + "      - name: inner-name\n";
    assertEquals("outer-name", PipelineGraphBuilder.extractMetadataName(yaml));
  }

  // ─── Test 4: reverse lookup with stale depends-on labels ─────────────────

  @Test
  void forResourceFiltersStaleLabelsViaAnnotationCrossCheck() throws SQLException {
    // Pipeline P used to depend on kafka1.X (label still present from a stale stamp), but the
    // current depends-on annotation only mentions kafka1.Y. A reverse lookup of kafka1.X must
    // skip P even though the label-selector matches.
    V1alpha1Pipeline stalePipeline = pipelineWithLabelButNoMatchingAnnotation();

    PipelineGraphBuilder builder = builder(
        list(), list(stalePipeline), list(), list());

    PipelineGraph graph = builder.forResource("kafka1", Arrays.asList("KAFKA", "x"), 2);

    assertEquals(GraphNode.Kind.EXTERNAL, graph.root().kind());
    // Only the root resource node should be present — the stale pipeline must have been filtered.
    assertEquals(1, graph.nodes().size(),
        "stale-label pipeline must be skipped via annotation cross-check; got: " + graph.nodes());
  }

  // ─── Builders for fixtures ───────────────────────────────────────────────

  private static PipelineGraphBuilder builder(
      List<V1alpha1View> views,
      List<V1alpha1Pipeline> pipelines,
      List<V1alpha1LogicalTable> logicalTables,
      List<V1alpha1TableTrigger> triggers) {
    return new PipelineGraphBuilder(
        new FakeK8sApi<V1alpha1View, V1alpha1ViewList>(new ArrayList<>(views)),
        new FakeK8sApi<V1alpha1Pipeline, V1alpha1PipelineList>(new ArrayList<>(pipelines)),
        new FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList>(new ArrayList<>(logicalTables)),
        new FakeK8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList>(new ArrayList<>(triggers)));
  }

  private static V1alpha1View view(String namespace, String name, boolean materialized, String uid) {
    V1alpha1View view = new V1alpha1View();
    view.setMetadata(new V1ObjectMeta().namespace(namespace).name(name).uid(uid));
    V1alpha1ViewSpec spec = new V1alpha1ViewSpec();
    spec.setMaterialized(materialized);
    view.setSpec(spec);
    return view;
  }

  private static V1alpha1LogicalTable logicalTable(String namespace, String name, String uid,
      Map<String, String> tiers) {
    V1alpha1LogicalTable lt = new V1alpha1LogicalTable();
    lt.setMetadata(new V1ObjectMeta().namespace(namespace).name(name).uid(uid));
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    spec.setTableName(name);
    Map<String, V1alpha1LogicalTableSpecTiers> tierMap = new LinkedHashMap<>();
    for (Map.Entry<String, String> e : tiers.entrySet()) {
      V1alpha1LogicalTableSpecTiers t = new V1alpha1LogicalTableSpecTiers();
      t.setDatabase(e.getValue());
      tierMap.put(e.getKey(), t);
    }
    spec.setTiers(tierMap);
    lt.setSpec(spec);
    return lt;
  }

  private static V1alpha1TableTrigger trigger(String namespace, String name, String ownerUid,
      String schedule, String sourceIdentifier, String sinkIdentifier) {
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger();
    Map<String, String> annotations = new HashMap<>();
    if (sourceIdentifier != null) {
      annotations.put(DependencyLabels.ANNOTATION_KEY_SOURCES, sourceIdentifier);
    }
    if (sinkIdentifier != null) {
      annotations.put(DependencyLabels.ANNOTATION_KEY_SINKS, sinkIdentifier);
    }
    V1ObjectMeta meta = new V1ObjectMeta().namespace(namespace).name(name).uid("trigger-uid")
        .annotations(annotations)
        .ownerReferences(Arrays.asList(new V1OwnerReference()
            .kind("LogicalTable").name("events").uid(ownerUid)));
    trigger.setMetadata(meta);
    V1alpha1TableTriggerSpec spec = new V1alpha1TableTriggerSpec();
    spec.setSchedule(schedule);
    trigger.setSpec(spec);
    return trigger;
  }


  private static V1alpha1Pipeline pipelineWithSourcesAndSink(String name, String ownerUid,
      String ownerKind, List<String> sourceIdentifiers, String sinkIdentifier) {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline();
    Map<String, String> labels = new HashMap<>();
    Map<String, String> annotations = new HashMap<>();

    for (String src : sourceIdentifiers) {
      labels.put("hoptimator.linkedin.com/depends-on-stub-" + src.hashCode(), src);
    }
    if (sinkIdentifier != null) {
      labels.put("hoptimator.linkedin.com/depends-on-stub-" + sinkIdentifier.hashCode(), sinkIdentifier);
    }
    annotations.put(DependencyLabels.ANNOTATION_KEY_SOURCES, String.join(",", sourceIdentifiers));
    if (sinkIdentifier != null) {
      annotations.put(DependencyLabels.ANNOTATION_KEY_SINKS, sinkIdentifier);
    }

    pipeline.setMetadata(new V1ObjectMeta()
        .namespace("ns").name(name).uid(name + "-uid")
        .labels(labels).annotations(annotations)
        .ownerReferences(Arrays.asList(new V1OwnerReference()
            .kind(ownerKind).name(ownerKindToName(ownerKind)).uid(ownerUid))));
    pipeline.setSpec(new V1alpha1PipelineSpec().sql("INSERT INTO " + sinkIdentifier
        + " SELECT * FROM " + String.join(",", sourceIdentifiers)));
    return pipeline;
  }

  private static String ownerKindToName(String kind) {
    // The fixtures use deterministic owner names; tests that need the name to match a specific
    // CRD wire it through ownerKindToName at build time.
    return "events";
  }

  private static V1alpha1Pipeline pipelineWithLabelButNoMatchingAnnotation() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline();
    Map<String, String> labels = new HashMap<>();
    Map<String, String> annotations = new HashMap<>();
    // Stale label says we depend on kafka1.X — but the directional annotations only mention
    // kafka1.Y. The cross-check should fail and the pipeline should be filtered out.
    labels.put(DependencyLabels.labelKey("kafka1", Arrays.asList("KAFKA", "x")),
        "kafka1_KAFKA.x");
    annotations.put(DependencyLabels.ANNOTATION_KEY_SOURCES, "kafka1_KAFKA.y");
    pipeline.setMetadata(new V1ObjectMeta()
        .namespace("ns").name("stale-pipeline").uid("stale-uid")
        .labels(labels).annotations(annotations));
    pipeline.setSpec(new V1alpha1PipelineSpec().sql("INSERT INTO kafka1.KAFKA.y SELECT * FROM elsewhere"));
    return pipeline;
  }

  // ─── Tiny helpers ────────────────────────────────────────────────────────

  @SafeVarargs
  private static <T> List<T> list(T... items) {
    return new ArrayList<>(Arrays.asList(items));
  }

  private static Map<String, String> linked(String... kvs) {
    Map<String, String> map = new LinkedHashMap<>();
    for (int i = 0; i < kvs.length; i += 2) {
      map.put(kvs[i], kvs[i + 1]);
    }
    return map;
  }

  private static GraphNode graphNodeByKind(PipelineGraph graph, GraphNode.Kind kind) {
    return graph.nodes().stream().filter(n -> n.kind() == kind).findFirst().orElse(null);
  }
}
