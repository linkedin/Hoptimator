package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.GraphEdge;
import com.linkedin.hoptimator.GraphNode;
import com.linkedin.hoptimator.PipelineGraph;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;


/**
 * Builds a {@link PipelineGraph} for visualization by walking K8s CRDs and the
 * {@code depends-on-*} dependency index stamped on Pipeline CRDs.
 *
 * <p>Three entry points:
 * <ul>
 *   <li>{@link #forView(String, String, int)} — graph rooted at a {@link V1alpha1View}.</li>
 *   <li>{@link #forLogicalTable(String, String, int)} — graph rooted at a {@link V1alpha1LogicalTable}.</li>
 *   <li>{@link #forResource(String, List, int)} — reverse lookup; graph rooted at an external resource.</li>
 * </ul>
 *
 * <p>Depth bounds traversal in both directions through the {@code depends-on-<slug>} label
 * selector. {@link #DEFAULT_DEPTH} = 2; the public surface caps depth at {@link #MAX_DEPTH}.
 */
public final class PipelineGraphBuilder {

  public static final int DEFAULT_DEPTH = 2;
  public static final int MAX_DEPTH = 5;

  private final K8sApi<V1alpha1View, V1alpha1ViewList> viewApi;
  private final K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi;
  private final K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi;
  private final K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi;
  private final K8sApi<V1alpha1Database, V1alpha1DatabaseList> databaseApi;

  public PipelineGraphBuilder(K8sContext context) {
    this(new K8sApi<>(context, K8sApiEndpoints.VIEWS),
        new K8sApi<>(context, K8sApiEndpoints.PIPELINES),
        new K8sApi<>(context, K8sApiEndpoints.LOGICAL_TABLES),
        new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS),
        new K8sApi<>(context, K8sApiEndpoints.DATABASES));
  }

  /** Package-private constructor for tests; accepts pre-built (or fake) K8s APIs. */
  PipelineGraphBuilder(K8sApi<V1alpha1View, V1alpha1ViewList> viewApi,
      K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi,
      K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi,
      K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi,
      K8sApi<V1alpha1Database, V1alpha1DatabaseList> databaseApi) {
    this.viewApi = viewApi;
    this.pipelineApi = pipelineApi;
    this.logicalTableApi = logicalTableApi;
    this.triggerApi = triggerApi;
    this.databaseApi = databaseApi;
  }

  // ─── Public entry points ─────────────────────────────────────────────────

  public PipelineGraph forView(String namespace, String name, int depth) throws SQLException {
    // Accept SQL-side identifiers (e.g. {@code VENICE.test-store$insert-partial}) — the CRD is
    // stored under a canonicalized name (lowercase, {@code _} stripped, {@code $} → {@code -},
    // dot-separated parts joined with {@code -}). Canonicalization is idempotent, so passing the
    // already-canonical CRD name still works.
    String crdName = K8sUtils.canonicalizeName(Arrays.asList(name.split("\\.")));
    V1alpha1View view = viewApi.get(namespace, crdName);
    boolean materialized = Boolean.TRUE.equals(view.getSpec().getMaterialized());
    GraphNode.View root = new GraphNode.View(namespace, crdName, materialized);

    int cappedDepth = cap(depth);
    Traversal t = new Traversal(cappedDepth);
    t.addNode(root);

    // Materialized views have a Pipeline owned by the View CRD whose sink matches the view's
    // canonicalized path. Discover it via owner-reference scan over Pipelines in the namespace,
    // and expand each — but do NOT walk further upstream. A !graph view query is anchored on a
    // specific view; the user expects "what this view does" (its pipeline + the resources it
    // reads and writes directly), not a transitive chain to whatever produces its inputs.
    // For the chain view, !graph table on the source is the right tool — the depth flag there
    // controls how far to walk.
    if (materialized) {
      String viewUid = view.getMetadata() == null ? null : view.getMetadata().getUid();
      for (V1alpha1Pipeline pipeline : pipelineApi.list()) {
        if (ownedBy(pipeline.getMetadata(), "View", crdName, viewUid)) {
          GraphNode.Pipeline pipeNode = t.expandPipelineDirected(pipeline, Direction.UPSTREAM, 0);
          if (pipeNode != null) {
            t.addEdge(new GraphEdge(root, pipeNode, GraphEdge.Type.OWNER_OF));
          }
        }
      }
    }

    return t.build(root);
  }

  public PipelineGraph forLogicalTable(String namespace, String name, int depth) throws SQLException {
    // Same canonicalization as forView — accept SQL-side identifiers and resolve to the
    // canonicalized CRD name.
    String crdName = K8sUtils.canonicalizeName(Arrays.asList(name.split("\\.")));
    V1alpha1LogicalTable lt = logicalTableApi.get(namespace, crdName);
    Map<String, String> tierMap = tierMap(lt.getSpec());
    GraphNode.LogicalTable root = new GraphNode.LogicalTable(namespace, crdName, tierMap);

    Traversal t = new Traversal(cap(depth));
    t.addNode(root);

    String ltUid = lt.getMetadata() == null ? null : lt.getMetadata().getUid();
    String ltName = crdName;

    // Implicit inter-tier pipelines: discovered by scanning all pipelines whose ownerReferences
    // point to this LogicalTable. Each gets an OWNER_OF edge from the LogicalTable so the
    // renderer nests them inside the LogicalTable subgraph. Pipeline expansion adds the actual
    // tier-physical externals — we group those into tier subgraphs in a second pass below.
    // Same scoping rule as forView: don't recurse beyond the owned pipelines' immediate
    // neighbors. The chain view belongs to !graph table.
    for (V1alpha1Pipeline pipeline : pipelineApi.list()) {
      if (ownedBy(pipeline.getMetadata(), "LogicalTable", ltName, ltUid)) {
        GraphNode.Pipeline pipeNode = t.expandPipelineDirected(pipeline, Direction.UPSTREAM, 0);
        if (pipeNode != null) {
          t.addEdge(new GraphEdge(root, pipeNode, GraphEdge.Type.OWNER_OF));
        }
      }
    }

    // OWNER_OF any external whose database matches a tier database — that's a tier-physical
    // resource of *this* LogicalTable, so the renderer nests it inside the appropriate tier
    // subgraph. Externals that aren't tier-physical (e.g. downstream consumers reached during
    // recursive expansion) stay outside.
    for (GraphNode node : new ArrayList<>(t.nodes())) {
      if (node instanceof GraphNode.External
          && tierMap.containsValue(((GraphNode.External) node).database())) {
        t.addEdge(new GraphEdge(root, node, GraphEdge.Type.OWNER_OF));
      }
    }

    // Owned triggers — same scan against the trigger API. Bridging triggers (those with both a
    // source and a sink annotation) render as `source -.-> trigger -.-> sink`. Single-node
    // triggers (Shape A) render with whichever endpoint is set.
    for (V1alpha1TableTrigger trigger : triggerApi.list()) {
      if (ownedBy(trigger.getMetadata(), "LogicalTable", ltName, ltUid)) {
        GraphNode.Trigger tNode = triggerNode(trigger);
        t.addNode(tNode);
        t.addEdge(new GraphEdge(root, tNode, GraphEdge.Type.OWNER_OF));
        attachTriggerFlowEdges(t, trigger, tNode);
      }
    }

    return t.build(root);
  }

  public PipelineGraph forResource(String database, List<String> path, int depth) throws SQLException {
    GraphNode.External root = externalNode(database, path);

    int cappedDepth = cap(depth);
    Traversal t = new Traversal(cappedDepth);
    t.addNode(root);
    // Reverse-lookup mode: walk in both directions from the root, but each recursion preserves
    // its own direction (upstream stays upstream, downstream stays downstream) so unrelated
    // siblings of intermediate pipelines don't leak in.
    t.expandFromResource(root, cappedDepth, Direction.UPSTREAM);
    t.expandFromResource(root, cappedDepth, Direction.DOWNSTREAM);

    return t.build(root);
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────

  private static int cap(int depth) {
    if (depth < 0) {
      return 0;
    }
    return Math.min(depth, MAX_DEPTH);
  }

  private static boolean ownedBy(V1ObjectMeta meta, String ownerKind, String ownerName, String ownerUid) {
    if (meta == null || meta.getOwnerReferences() == null) {
      return false;
    }
    for (V1OwnerReference ref : meta.getOwnerReferences()) {
      if (!ownerKind.equals(ref.getKind())) {
        continue;
      }
      // UID match is strictest; fall back to name match (matches the dep-guard's relaxation).
      if (ownerUid != null && ownerUid.equals(ref.getUid())) {
        return true;
      }
      if (ownerName.equals(ref.getName())) {
        return true;
      }
    }
    return false;
  }

  private GraphNode.External externalNode(String database, List<String> path) {
    String driver = lookupDriver(database);
    return new GraphNode.External(database, path, driver);
  }

  private String lookupDriver(String database) {
    try {
      V1alpha1Database db = databaseApi.get(database);
      if (db == null || db.getSpec() == null) {
        return null;
      }
      V1alpha1DatabaseSpec spec = db.getSpec();
      return spec.getDriver();
    } catch (Exception ignored) {
      // Database CRD doesn't exist or wasn't reachable — render the External without a driver
      // icon rather than failing the whole graph.
      return null;
    }
  }

  private static Map<String, String> tierMap(V1alpha1LogicalTableSpec spec) {
    Map<String, String> out = new LinkedHashMap<>();
    if (spec == null || spec.getTiers() == null) {
      return out;
    }
    for (Map.Entry<String, V1alpha1LogicalTableSpecTiers> e : spec.getTiers().entrySet()) {
      out.put(e.getKey(), e.getValue() == null ? null : e.getValue().getDatabase());
    }
    return out;
  }

  /**
   * Pull the workhorse {@code kind:} out of the rendered job YAML inside
   * {@code V1alpha1PipelineSpec.yaml}. The yaml stream interleaves data resources (KafkaTopic,
   * VeniceStore, etc.) with the actual execution artifact. We pick the first kind whose name
   * ends in {@code Job} — covers FlinkSessionJob, FlinkBeamSqlJob, BeamSqlJob, batch/v1 Job,
   * etc. without having to maintain an allowlist. Returns null if no Job-suffixed kind is found.
   */
  static String extractJobKind(String yaml) {
    if (yaml == null || yaml.isEmpty()) {
      return null;
    }
    java.util.regex.Matcher m = java.util.regex.Pattern
        .compile("(?m)^kind:\\s*(\\S*Job)\\s*$").matcher(yaml);
    return m.find() ? m.group(1) : null;
  }

  /** Cheap engine inference from the job kind + raw YAML. Returns null when uncertain. */
  static String inferEngine(String jobKind, String yaml) {
    if (jobKind == null) {
      return null;
    }
    String kindLower = jobKind.toLowerCase();
    boolean mentionsBeam = kindLower.contains("beam")
        || (yaml != null && yaml.toLowerCase().contains("beam"));
    if (mentionsBeam && kindLower.contains("flink")) {
      return "Flink Beam";
    }
    if (mentionsBeam) {
      return "Beam";
    }
    if (kindLower.contains("flink")) {
      return "Flink";
    }
    if (kindLower.contains("spark")) {
      return "Spark";
    }
    return null;
  }

  private static GraphNode.Trigger triggerNode(V1alpha1TableTrigger trigger) {
    String namespace = trigger.getMetadata() == null ? null : trigger.getMetadata().getNamespace();
    String name = trigger.getMetadata() == null ? "<unknown>" : trigger.getMetadata().getName();
    String schedule = trigger.getSpec() == null ? null : trigger.getSpec().getSchedule();
    boolean paused = trigger.getSpec() != null && Boolean.TRUE.equals(trigger.getSpec().getPaused());
    String jobTemplate = annotationValue(trigger, K8sTriggerDeployer.JOB_TEMPLATE_ANNOTATION);
    String yaml = trigger.getSpec() == null ? null : trigger.getSpec().getYaml();
    String jobKind = extractJobKind(yaml);
    String jobName = extractMetadataName(yaml);
    String containerName = extractFirstContainerName(yaml);
    return new GraphNode.Trigger(namespace, name, schedule, paused,
        jobTemplate, jobKind, jobName, containerName);
  }

  /**
   * Attach trigger flow edges. Reads the {@code depends-on-sources} and {@code depends-on-sink}
   * annotations and emits dotted edges in the data-flow direction:
   * {@code source --(dotted)--> trigger --(dotted)--> sink}. If only one endpoint resolves to
   * an External in the graph, only that edge is emitted (Shape A).
   */
  private static void attachTriggerFlowEdges(PipelineGraphBuilder.Traversal t,
      V1alpha1TableTrigger trigger, GraphNode.Trigger tNode) {
    String sourceIdentifier = triggerSourceIdentifier(trigger);
    if (sourceIdentifier != null) {
      GraphNode.External source = t.findExternalByIdentifier(sourceIdentifier);
      if (source != null) {
        t.addEdge(new GraphEdge(source, tNode, GraphEdge.Type.TRIGGERS));
      }
    }
    String sinkIdentifier = triggerSinkIdentifier(trigger);
    if (sinkIdentifier != null) {
      GraphNode.External sink = t.findExternalByIdentifier(sinkIdentifier);
      if (sink != null) {
        t.addEdge(new GraphEdge(tNode, sink, GraphEdge.Type.TRIGGERS));
      }
    }
  }

  /** Pull the trigger's source identifier from the {@code depends-on-sources} annotation. */
  private static String triggerSourceIdentifier(V1alpha1TableTrigger trigger) {
    String value = annotationValue(trigger, PipelineDependencyLabels.ANNOTATION_KEY_SOURCES);
    if (value == null || value.isEmpty()) {
      return null;
    }
    Set<String> ids = PipelineDependencyLabels.parseAnnotation(value);
    return ids.isEmpty() ? null : ids.iterator().next();
  }

  /** Pull the trigger's sink identifier from the {@code depends-on-sink} annotation. */
  private static String triggerSinkIdentifier(V1alpha1TableTrigger trigger) {
    return annotationValue(trigger, PipelineDependencyLabels.ANNOTATION_KEY_SINK);
  }

  private static String annotationValue(V1alpha1TableTrigger trigger, String key) {
    if (trigger.getMetadata() == null || trigger.getMetadata().getAnnotations() == null) {
      return null;
    }
    return trigger.getMetadata().getAnnotations().get(key);
  }

  /** First {@code metadata.name:} line from the rendered yaml; nullable. */
  static String extractMetadataName(String yaml) {
    if (yaml == null || yaml.isEmpty()) {
      return null;
    }
    java.util.regex.Matcher m = java.util.regex.Pattern
        .compile("(?m)^\\s+name:\\s*(\\S+)\\s*$").matcher(yaml);
    return m.find() ? m.group(1) : null;
  }

  /** First container's {@code name:} in the {@code containers:} list; nullable. */
  static String extractFirstContainerName(String yaml) {
    if (yaml == null || yaml.isEmpty()) {
      return null;
    }
    int containersIdx = yaml.indexOf("containers:");
    if (containersIdx < 0) {
      return null;
    }
    java.util.regex.Matcher m = java.util.regex.Pattern
        .compile("(?m)^\\s*-\\s*name:\\s*(\\S+)\\s*$").matcher(yaml.substring(containersIdx));
    return m.find() ? m.group(1) : null;
  }


  // ─── Traversal state holder ──────────────────────────────────────────────

  /**
   * Direction of recursive expansion through pipelines/triggers.
   *
   * <ul>
   *   <li>{@link #UPSTREAM} — follow producers. From an external, find pipelines/triggers where
   *       it appears as the *sink*; from a pipeline, walk to its sources.</li>
   *   <li>{@link #DOWNSTREAM} — follow consumers. From an external, find pipelines/triggers
   *       where it appears as a *source*; from a pipeline, walk to its sink.</li>
   * </ul>
   *
   * <p>Direction stays sticky across one recursive walk, so unrelated siblings of an
   * intermediate pipeline don't get pulled in via the opposite direction.
   */
  enum Direction { UPSTREAM, DOWNSTREAM }

  private final class Traversal {
    private final int maxDepth;
    private final Map<String, GraphNode> nodes = new LinkedHashMap<>();
    private final Set<GraphEdge> edges = new LinkedHashSet<>();
    private final Set<String> expandedResources = new HashSet<>();
    private final Set<String> expandedPipelines = new HashSet<>();

    Traversal(int maxDepth) {
      this.maxDepth = maxDepth;
    }

    void addNode(GraphNode node) {
      nodes.putIfAbsent(node.id(), node);
    }

    void addEdge(GraphEdge edge) {
      edges.add(edge);
    }

    /** All nodes added so far, in insertion order. */
    Collection<GraphNode> nodes() {
      return nodes.values();
    }

    PipelineGraph build(GraphNode root) {
      return new PipelineGraph(root, new LinkedHashSet<>(nodes.values()), edges);
    }

    /** Find an {@link GraphNode.External} matching a {@code <database>_<dot.path>} identifier. */
    GraphNode.External findExternalByIdentifier(String identifier) {
      if (identifier == null) {
        return null;
      }
      for (GraphNode n : nodes.values()) {
        if (n instanceof GraphNode.External) {
          GraphNode.External e = (GraphNode.External) n;
          if (identifier.equals(PipelineDependencyLabels.identifier(e.database(), e.path()))) {
            return e;
          }
        }
      }
      return null;
    }


    /**
     * Fan out from an external resource in one direction at a time. {@link Direction#UPSTREAM}
     * finds pipelines/triggers that *produce* {@code resource} (where it appears as the sink);
     * {@link Direction#DOWNSTREAM} finds those that *consume* it (where it appears as a source).
     * Recursion stays in the same direction — once you're walking upstream, an intermediate
     * pipeline's siblings don't get pulled in via their downstream side.
     */
    void expandFromResource(GraphNode.External resource, int remainingDepth, Direction direction)
        throws SQLException {
      String key = direction.name() + "/" + resource.database() + "/" + String.join(".", resource.path());
      if (!expandedResources.add(key) || remainingDepth <= 0) {
        return;
      }
      String labelKey = PipelineDependencyLabels.labelKey(resource.database(), resource.path());
      String identifier = PipelineDependencyLabels.identifier(resource.database(), resource.path());

      Collection<V1alpha1Pipeline> matches = pipelineApi.select(labelKey);
      for (V1alpha1Pipeline pipeline : matches) {
        if (!annotationMentions(pipeline, identifier)) {
          // Stale label or hash collision — ignore.
          continue;
        }
        Set<String> sources = parseAnnotation(pipeline, PipelineDependencyLabels.ANNOTATION_KEY_SOURCES);
        String sink = annotationValue(pipeline, PipelineDependencyLabels.ANNOTATION_KEY_SINK);
        if (direction == Direction.UPSTREAM) {
          // Only pipelines that *produce* resource (resource is the sink). Pipelines that
          // merely consume resource are downstream — skip them in upstream walks.
          if (!identifier.equals(sink)) {
            continue;
          }
        } else {
          // Only pipelines that *consume* resource (resource is one of the sources).
          if (!sources.contains(identifier)) {
            continue;
          }
        }
        expandPipelineDirected(pipeline, direction, remainingDepth);
      }

      // Triggers carry the same `depends-on-<slug>` labels. Same direction filter:
      //   UPSTREAM   — trigger's sink == resource (trigger produces resource).
      //   DOWNSTREAM — trigger's source == resource (trigger consumes resource).
      Collection<V1alpha1TableTrigger> triggerMatches = triggerApi.select(labelKey);
      for (V1alpha1TableTrigger trigger : triggerMatches) {
        String triggerSource = triggerSourceIdentifier(trigger);
        String triggerSink = triggerSinkIdentifier(trigger);
        if (!identifier.equals(triggerSource) && !identifier.equals(triggerSink)) {
          // Annotation cross-check — skip stale labels and slug collisions.
          continue;
        }
        if (direction == Direction.UPSTREAM && !identifier.equals(triggerSink)) {
          continue;
        }
        if (direction == Direction.DOWNSTREAM && !identifier.equals(triggerSource)) {
          continue;
        }
        GraphNode.Trigger tNode = triggerNode(trigger);
        addNode(tNode);
        if (direction == Direction.UPSTREAM) {
          // Trigger produces resource; arrow points into resource.
          addEdge(new GraphEdge(tNode, resource, GraphEdge.Type.TRIGGERS));
          // Walk further upstream from the trigger's source (if any) — that's what feeds
          // the trigger.
          if (triggerSource != null) {
            GraphNode.External srcExt = externalFromIdentifier(triggerSource);
            addNode(srcExt);
            addEdge(new GraphEdge(srcExt, tNode, GraphEdge.Type.TRIGGERS));
            expandFromResource(srcExt, remainingDepth - 1, Direction.UPSTREAM);
          }
        } else {
          // Trigger consumes resource; arrow points away from resource.
          addEdge(new GraphEdge(resource, tNode, GraphEdge.Type.TRIGGERS));
          if (triggerSink != null) {
            GraphNode.External sinkExt = externalFromIdentifier(triggerSink);
            addNode(sinkExt);
            addEdge(new GraphEdge(tNode, sinkExt, GraphEdge.Type.TRIGGERS));
            expandFromResource(sinkExt, remainingDepth - 1, Direction.DOWNSTREAM);
          }
        }
      }
    }

    /**
     * Ingest a Pipeline CRD into the graph. Reads source/sink annotations, creates edges,
     * and schedules transitive expansion from the other endpoints. Returns the Pipeline node
     * so the caller can attach owner-of edges.
     */
    GraphNode.Pipeline expandPipeline(V1alpha1Pipeline pipeline, GraphNode rootContext) throws SQLException {
      String pipeId = pipelineId(pipeline);
      if (pipeId == null || !expandedPipelines.add(pipeId)) {
        return null;
      }
      GraphNode.Pipeline pipeNode = pipelineNode(pipeline);
      addNode(pipeNode);

      Set<String> sources = parseAnnotation(pipeline, PipelineDependencyLabels.ANNOTATION_KEY_SOURCES);
      String sink = annotationValue(pipeline, PipelineDependencyLabels.ANNOTATION_KEY_SINK);

      for (String id : sources) {
        GraphNode.External ext = externalFromIdentifier(id);
        addNode(ext);
        addEdge(new GraphEdge(ext, pipeNode, GraphEdge.Type.DEPENDS_ON_SOURCE));
      }
      if (sink != null) {
        GraphNode.External ext = externalFromIdentifier(sink);
        addNode(ext);
        addEdge(new GraphEdge(pipeNode, ext, GraphEdge.Type.DEPENDS_ON_SINK));
      }
      return pipeNode;
    }

    /**
     * Ingest a pipeline and recursively expand in one direction. The pipeline node is added with
     * all its source/sink edges (so the user always sees the pipeline's full shape), but the
     * recursion only walks further in {@code direction}:
     *
     * <ul>
     *   <li>{@link Direction#UPSTREAM} — recurse upstream from each of the pipeline's sources.
     *       The sink is rendered as a leaf node here, since we don't follow it downstream.</li>
     *   <li>{@link Direction#DOWNSTREAM} — recurse downstream from the pipeline's sink. The
     *       sources stay as leaves.</li>
     * </ul>
     */
    GraphNode.Pipeline expandPipelineDirected(V1alpha1Pipeline pipeline, Direction direction,
        int remainingDepth) throws SQLException {
      GraphNode.Pipeline pipeNode = expandPipeline(pipeline, null);
      if (pipeNode == null) {
        return null;
      }
      if (remainingDepth <= 0) {
        return pipeNode;
      }
      Set<String> sources = parseAnnotation(pipeline, PipelineDependencyLabels.ANNOTATION_KEY_SOURCES);
      String sink = annotationValue(pipeline, PipelineDependencyLabels.ANNOTATION_KEY_SINK);
      if (direction == Direction.UPSTREAM) {
        for (String id : sources) {
          GraphNode.External srcExt = externalFromIdentifier(id);
          expandFromResource(srcExt, remainingDepth - 1, Direction.UPSTREAM);
        }
      } else {
        if (sink != null) {
          GraphNode.External sinkExt = externalFromIdentifier(sink);
          expandFromResource(sinkExt, remainingDepth - 1, Direction.DOWNSTREAM);
        }
      }
      return pipeNode;
    }

    private GraphNode.External externalFromIdentifier(String identifier) throws SQLException {
      // Identifier shape: <database>_<dot.joined.path>. Split on the first underscore.
      int idx = identifier.indexOf('_');
      if (idx < 0) {
        return externalNode(identifier, Collections.emptyList());
      }
      String database = identifier.substring(0, idx);
      String pathStr = identifier.substring(idx + 1);
      List<String> path = pathStr.isEmpty() ? Collections.emptyList()
          : Arrays.asList(pathStr.split("\\."));
      return externalNode(database, path);
    }

    private GraphNode.Pipeline pipelineNode(V1alpha1Pipeline pipeline) {
      String namespace = pipeline.getMetadata() == null ? null : pipeline.getMetadata().getNamespace();
      String name = pipeline.getMetadata() == null ? "<unknown>" : pipeline.getMetadata().getName();
      String sql = pipeline.getSpec() == null ? null : pipeline.getSpec().getSql();
      String yaml = pipeline.getSpec() == null ? null : pipeline.getSpec().getYaml();
      String jobKind = extractJobKind(yaml);
      String engine = inferEngine(jobKind, yaml);
      return new GraphNode.Pipeline(namespace, name, sql, jobKind, engine);
    }

    private String pipelineId(V1alpha1Pipeline pipeline) {
      V1ObjectMeta meta = pipeline.getMetadata();
      if (meta == null) {
        return null;
      }
      return meta.getNamespace() + "/" + meta.getName();
    }

    private Set<String> parseAnnotation(V1alpha1Pipeline pipeline, String key) {
      String value = annotationValue(pipeline, key);
      return value == null ? Collections.emptySet() : PipelineDependencyLabels.parseAnnotation(value);
    }

    private String annotationValue(V1alpha1Pipeline pipeline, String key) {
      V1ObjectMeta meta = pipeline.getMetadata();
      if (meta == null || meta.getAnnotations() == null) {
        return null;
      }
      return meta.getAnnotations().get(key);
    }

    private boolean annotationMentions(V1alpha1Pipeline pipeline, String identifier) {
      // Confirm the label-selector match is real — the resource must appear in either the
      // sources or sink annotation. Used to filter out hash collisions and stale labels.
      Set<String> sources = parseAnnotation(pipeline, PipelineDependencyLabels.ANNOTATION_KEY_SOURCES);
      if (sources.contains(identifier)) {
        return true;
      }
      String sink = annotationValue(pipeline, PipelineDependencyLabels.ANNOTATION_KEY_SINK);
      return identifier.equals(sink);
    }
  }
}
