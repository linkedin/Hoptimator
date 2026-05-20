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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.PipelineGraph;
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
 *   <li>{@link #forView(String)} — graph rooted at a {@link V1alpha1View}.</li>
 *   <li>{@link #forLogicalTable(String)} — graph rooted at a {@link V1alpha1LogicalTable}.</li>
 *   <li>{@link #forResource(String, List, int)} — reverse lookup; graph rooted at an external resource.</li>
 * </ul>
 *
 * <p>Depth bounds traversal in both directions through the {@code depends-on-<slug>} label
 * selector. Callers pick whatever depth they want; the builder only floors negatives at 0
 * so the recursion always terminates.
 */
public final class PipelineGraphBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineGraphBuilder.class);

  private final K8sApi<V1alpha1View, V1alpha1ViewList> viewApi;
  private final K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi;
  private final K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi;
  private final K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi;

  public PipelineGraphBuilder(K8sContext context) {
    this(new K8sApi<>(context, K8sApiEndpoints.VIEWS),
        new K8sApi<>(context, K8sApiEndpoints.PIPELINES),
        new K8sApi<>(context, K8sApiEndpoints.LOGICAL_TABLES),
        new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS));
  }

  /** Package-private constructor for tests; accepts pre-built (or fake) K8s APIs. */
  PipelineGraphBuilder(K8sApi<V1alpha1View, V1alpha1ViewList> viewApi,
      K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi,
      K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi,
      K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi) {
    this.viewApi = viewApi;
    this.pipelineApi = pipelineApi;
    this.logicalTableApi = logicalTableApi;
    this.triggerApi = triggerApi;
  }

  // ─── Public entry points ─────────────────────────────────────────────────

  public PipelineGraph forView(String name) throws SQLException {
    // Accept SQL-side identifiers (e.g. {@code VENICE.test-store$insert-partial}) — the CRD is
    // stored under a canonicalized name (lowercase, {@code _} stripped, {@code $} → {@code -},
    // dot-separated parts joined with {@code -}). Canonicalization is idempotent, so passing the
    // already-canonical CRD name still works.
    String crdName = K8sUtils.canonicalizeName(Arrays.asList(name.split("\\.")));
    V1alpha1View view = viewApi.get(crdName);
    if (view.getSpec() == null) {
      throw new SQLException("view " + crdName + " not found");
    }
    boolean materialized = Boolean.TRUE.equals(view.getSpec().getMaterialized());
    GraphNode.View root = new GraphNode.View(crdName, materialized);

    Traversal t = new Traversal();
    t.addNode(root);

    // Materialized views own a Pipeline with the same CRD name (see K8sMaterializedViewDeployer).
    // Look it up directly — avoids a namespace-wide LIST. The owner-ref check still runs to defend
    // against a coincidentally-named pipeline that isn't actually owned by this view (e.g. stale
    // pipeline from a recreated view CRD with a fresh UID). Expansion is single-hop: "what this
    // view does," not the full upstream chain — for that, run !graph on a source identifier
    // (Resource targets honor depth).
    if (materialized) {
      String viewUid = view.getMetadata() == null ? null : view.getMetadata().getUid();
      V1alpha1Pipeline pipeline = pipelineApi.getIfExists(crdName);
      if (pipeline != null && ownedBy(pipeline.getMetadata(), "View", crdName, viewUid)) {
        GraphNode.Pipeline pipeNode = t.expandPipelineDirected(pipeline, Direction.UPSTREAM, 0);
        if (pipeNode != null) {
          t.addEdge(new GraphEdge(root, pipeNode, GraphEdge.Type.OWNER_OF));
        }
      }
    }

    return t.build(root);
  }

  public PipelineGraph forLogicalTable(String name) throws SQLException {
    // Same canonicalization as forView — accept SQL-side identifiers and resolve to the
    // canonicalized CRD name.
    String crdName = K8sUtils.canonicalizeName(Arrays.asList(name.split("\\.")));
    V1alpha1LogicalTable lt = logicalTableApi.get(crdName);
    Map<String, String> tierMap = tierMap(lt.getSpec());
    GraphNode.LogicalTable root = new GraphNode.LogicalTable(crdName, tierMap);

    Traversal t = new Traversal();
    t.addNode(root);

    String ltUid = lt.getMetadata() == null ? null : lt.getMetadata().getUid();
    String tableName = lt.getSpec() == null ? null : lt.getSpec().getTableName();

    // Implicit inter-tier pipelines: the LogicalTable deployer names them via
    // {@link LogicalTableNames#pipelineName} — derive every (from, to) candidate from the tier
    // list and probe each by name. The deployer only actually creates a subset of pairs (today:
    // nearline→online, nearline→offline), so most candidate GETs miss with 404 — that's fine, and
    // it keeps the visualizer decoupled from the deployer's pair-selection rules: whatever the
    // deployer actually creates is what we render. Owner-ref check defends against a coincidentally
    // -named pipeline owned by something else (or a stale CRD with a different UID). Expansion is
    // single-hop — for the chain view, run !graph on a source identifier (Resource targets honor
    // depth).
    if (tableName != null) {
      List<String> tierNames = new ArrayList<>(tierMap.keySet());
      for (String fromTier : tierNames) {
        for (String toTier : tierNames) {
          if (fromTier.equals(toTier)) {
            continue;
          }
          String candidate = LogicalTableNames.pipelineName(tableName, fromTier, toTier);
          V1alpha1Pipeline pipeline = pipelineApi.getIfExists(candidate);
          if (pipeline != null && ownedBy(pipeline.getMetadata(), "LogicalTable", crdName, ltUid)) {
            GraphNode.Pipeline pipeNode = t.expandPipelineDirected(pipeline, Direction.UPSTREAM, 0);
            if (pipeNode != null) {
              t.addEdge(new GraphEdge(root, pipeNode, GraphEdge.Type.OWNER_OF));
            }
          }
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

    // Owned trigger: the LogicalTable deployer creates at most one trigger per table
    // ({@link LogicalTableNames#triggerName}), gated on an offline tier being present. We probe
    // unconditionally — a 404 simply means there's no offline tier in this table's spec, which is
    // exactly the absence we want to render.
    if (tableName != null) {
      String triggerCandidate = LogicalTableNames.triggerName(tableName);
      V1alpha1TableTrigger trigger = triggerApi.getIfExists(triggerCandidate);
      if (trigger != null && ownedBy(trigger.getMetadata(), "LogicalTable", crdName, ltUid)) {
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

    int cappedDepth = Math.max(depth, 0);
    Traversal t = new Traversal();
    t.addNode(root);
    // Reverse-lookup mode: walk in both directions from the root, but each recursion preserves
    // its own direction (upstream stays upstream, downstream stays downstream) so unrelated
    // siblings of intermediate pipelines don't leak in.
    t.expandFromResource(root, cappedDepth, Direction.UPSTREAM);
    t.expandFromResource(root, cappedDepth, Direction.DOWNSTREAM);

    return t.build(root);
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────

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
    return new GraphNode.External(database, path);
  }

  private static Map<String, String> tierMap(V1alpha1LogicalTableSpec spec) {
    Map<String, String> out = new LinkedHashMap<>();
    if (spec == null || spec.getTiers() == null) {
      return out;
    }
    for (Map.Entry<String, V1alpha1LogicalTableSpecTiers> e : spec.getTiers().entrySet()) {
      // Skip tiers with no resolved database — the downstream renderer's tier lookup keys on the
      // database value, so a null binding can't be matched and would silently become a malformed
      // subgraph. Better to drop the malformed entry and surface the rest of the LogicalTable.
      String tierDb = e.getValue() == null ? null : e.getValue().getDatabase();
      if (tierDb == null) {
        continue;
      }
      out.put(e.getKey(), tierDb);
    }
    return out;
  }

  /**
   * Pull the workhorse {@code kind:} out of the rendered job YAML inside
   * {@code V1alpha1PipelineSpec.yaml}. The yaml stream interleaves data resources (KafkaTopic,
   * VeniceStore, etc.) with the actual execution artifact. We pick the first kind whose name
   * ends in {@code Job} — covers FlinkSessionJob, SqlJob, EtlJob, batch/v1 Job,
   * etc. without having to maintain an allowlist. Returns null if no Job-suffixed kind is found.
   */
  static String extractJobKind(String yaml) {
    if (yaml == null || yaml.isEmpty()) {
      return null;
    }
    Matcher m = Pattern.compile("(?m)^kind:\\s*(\\S*Job)\\s*$").matcher(yaml);
    return m.find() ? m.group(1) : null;
  }

  /**
   * Cheap engine inference from the job kind. Used for non-{@code SqlJob} job kinds (e.g.
   * {@code FlinkSessionJob} where the engine is encoded in the kind name.
   * For {@code SqlJob}, use {@link #extractSqlJobField} on {@code spec.dialect} directly —
   * the engine is a structured spec field, not something to infer.
   */
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

  /**
   * Pull a scalar field from the {@code spec:} block of the {@code SqlJob} doc in a rendered
   * pipeline YAML. The rendered YAML interleaves data resources (KafkaTopic, VeniceStore, …) with
   * the SqlJob, so we scope the search to the slice after {@code kind: SqlJob} and before the
   * next document boundary. Returns null when the YAML doesn't contain a SqlJob or the field is
   * absent. Intended for the CRD's typed-enum fields ({@code dialect}, {@code executionMode}),
   * which the deployer always writes as bare scalars.
   */
  static String extractSqlJobField(String yaml, String fieldName) {
    if (yaml == null || yaml.isEmpty() || fieldName == null) {
      return null;
    }
    Matcher kindMatcher = Pattern.compile("(?m)^kind:\\s*SqlJob\\s*$").matcher(yaml);
    if (!kindMatcher.find()) {
      return null;
    }
    String tail = yaml.substring(kindMatcher.start());
    int boundary = tail.indexOf("\n---");
    if (boundary >= 0) {
      tail = tail.substring(0, boundary);
    }
    Matcher fieldMatcher = Pattern.compile("(?m)^\\s+" + Pattern.quote(fieldName) + ":\\s*(\\S+)\\s*$")
        .matcher(tail);
    return fieldMatcher.find() ? fieldMatcher.group(1) : null;
  }

  private static GraphNode.Trigger triggerNode(V1alpha1TableTrigger trigger) {
    String name = trigger.getMetadata() == null ? "<unknown>" : trigger.getMetadata().getName();
    String schedule = trigger.getSpec() == null ? null : trigger.getSpec().getSchedule();
    boolean paused = trigger.getSpec() != null && Boolean.TRUE.equals(trigger.getSpec().getPaused());
    String yaml = trigger.getSpec() == null ? null : trigger.getSpec().getYaml();
    String containerName = extractFirstContainerName(yaml);
    String jobTemplate = extractJobTemplateName(name, yaml);
    return new GraphNode.Trigger(name, schedule, paused, jobTemplate, containerName);
  }

  /**
   * Derive the JobTemplate name from the rendered Job's {@code metadata.name}. The deployer
   * builds it as {@code <triggerName>-<templateName>} (both canonicalized), so we strip the
   * trigger-name prefix to recover the template name. Returns null if the prefix doesn't match
   * (the YAML doesn't follow the convention, or the name isn't present).
   */
  static String extractJobTemplateName(String triggerName, String yaml) {
    if (triggerName == null || yaml == null || yaml.isEmpty()) {
      return null;
    }
    String jobName = extractMetadataName(yaml);
    if (jobName == null) {
      return null;
    }
    String prefix = triggerName + "-";
    if (!jobName.startsWith(prefix) || jobName.length() == prefix.length()) {
      return null;
    }
    return jobName.substring(prefix.length());
  }

  /** Top-level {@code metadata.name:} value in a Kubernetes YAML doc; nullable. */
  static String extractMetadataName(String yaml) {
    if (yaml == null || yaml.isEmpty()) {
      return null;
    }
    int metadataIdx = yaml.indexOf("metadata:");
    if (metadataIdx < 0) {
      return null;
    }
    Matcher m = Pattern.compile("(?m)^\\s+name:\\s*(\\S+)\\s*$")
        .matcher(yaml.substring(metadataIdx));
    return m.find() ? m.group(1) : null;
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
    return firstAnnotationIdentifier(trigger, DependencyLabels.ANNOTATION_KEY_SOURCES);
  }

  /** Pull the trigger's sink identifier from the {@code depends-on-sinks} annotation. Today a
   *  trigger has at most one sink, so we return the first; if/when triggers carry multiple sinks
   *  this needs to grow into an iteration. */
  private static String triggerSinkIdentifier(V1alpha1TableTrigger trigger) {
    return firstAnnotationIdentifier(trigger, DependencyLabels.ANNOTATION_KEY_SINKS);
  }

  /**
   * First identifier parsed from a {@code depends-on-*} annotation on the trigger, or null when
   * the annotation is missing/empty. {@link DependencyLabels#parseAnnotation(String)} treats null
   * inputs as empty, so no caller-side null guard is needed.
   */
  private static String firstAnnotationIdentifier(V1alpha1TableTrigger trigger, String annotationKey) {
    Set<String> ids = DependencyLabels.parseAnnotation(annotationValue(trigger, annotationKey));
    return ids.isEmpty() ? null : ids.iterator().next();
  }

  private static String annotationValue(V1alpha1TableTrigger trigger, String key) {
    if (trigger.getMetadata() == null || trigger.getMetadata().getAnnotations() == null) {
      return null;
    }
    return trigger.getMetadata().getAnnotations().get(key);
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
    Matcher m = Pattern.compile("(?m)^\\s*-\\s*name:\\s*(\\S+)\\s*$")
        .matcher(yaml.substring(containersIdx));
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
    private final Map<String, GraphNode> nodes = new LinkedHashMap<>();
    private final Set<GraphEdge> edges = new LinkedHashSet<>();
    private final Set<String> expandedResources = new HashSet<>();
    private final Set<String> expandedPipelines = new HashSet<>();

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
          if (identifier.equals(DependencyLabels.identifier(e.database(), e.path()))) {
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
      String labelKey = DependencyLabels.labelKey(resource.database(), resource.path());
      String identifier = DependencyLabels.identifier(resource.database(), resource.path());

      Collection<V1alpha1Pipeline> matches = pipelineApi.select(labelKey);
      for (V1alpha1Pipeline pipeline : matches) {
        if (!annotationMentions(pipeline, identifier)) {
          // Stale label or hash collision — ignore.
          continue;
        }
        Set<String> sources = parseAnnotation(pipeline, DependencyLabels.ANNOTATION_KEY_SOURCES);
        Set<String> sinks = parseAnnotation(pipeline, DependencyLabels.ANNOTATION_KEY_SINKS);
        if (direction == Direction.UPSTREAM) {
          // Only pipelines that *produce* resource (resource is one of the sinks). Pipelines
          // that merely consume resource are downstream — skip them in upstream walks.
          if (!sinks.contains(identifier)) {
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
    GraphNode.Pipeline expandPipeline(V1alpha1Pipeline pipeline) {
      String pipeId = pipelineId(pipeline);
      if (pipeId == null || !expandedPipelines.add(pipeId)) {
        return null;
      }
      GraphNode.Pipeline pipeNode = pipelineNode(pipeline);
      addNode(pipeNode);

      Set<String> sources = parseAnnotation(pipeline, DependencyLabels.ANNOTATION_KEY_SOURCES);
      Set<String> sinks = parseAnnotation(pipeline, DependencyLabels.ANNOTATION_KEY_SINKS);

      for (String id : sources) {
        GraphNode.External ext = externalFromIdentifier(id);
        addNode(ext);
        addEdge(new GraphEdge(ext, pipeNode, GraphEdge.Type.DEPENDS_ON_SOURCE));
      }
      for (String id : sinks) {
        GraphNode.External ext = externalFromIdentifier(id);
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
      GraphNode.Pipeline pipeNode = expandPipeline(pipeline);
      if (pipeNode == null) {
        return null;
      }
      if (remainingDepth <= 0) {
        return pipeNode;
      }
      Set<String> sources = parseAnnotation(pipeline, DependencyLabels.ANNOTATION_KEY_SOURCES);
      Set<String> sinks = parseAnnotation(pipeline, DependencyLabels.ANNOTATION_KEY_SINKS);
      if (direction == Direction.UPSTREAM) {
        for (String id : sources) {
          GraphNode.External srcExt = externalFromIdentifier(id);
          expandFromResource(srcExt, remainingDepth - 1, Direction.UPSTREAM);
        }
      } else {
        for (String id : sinks) {
          GraphNode.External sinkExt = externalFromIdentifier(id);
          expandFromResource(sinkExt, remainingDepth - 1, Direction.DOWNSTREAM);
        }
      }
      return pipeNode;
    }

    private GraphNode.External externalFromIdentifier(String identifier) {
      // Identifier shape: <database>_<dot.joined.path>. Split on the first underscore.
      int idx = identifier.indexOf('_');
      if (idx < 0) {
        // Missing the separator — likely a stale or hand-edited depends-on annotation. Surface as
        // a degraded External (database == identifier, empty path) so we render *something*, but
        // log so operators can spot the corruption in production.
        LOG.warn("depends-on identifier {} is malformed (missing '_' database/path separator); "
            + "rendering as degraded External with no path", identifier);
        return externalNode(identifier, Collections.emptyList());
      }
      String database = identifier.substring(0, idx);
      String pathStr = identifier.substring(idx + 1);
      List<String> path = pathStr.isEmpty() ? Collections.emptyList()
          : Arrays.asList(pathStr.split("\\."));
      return externalNode(database, path);
    }

    private GraphNode.Pipeline pipelineNode(V1alpha1Pipeline pipeline) {
      String name = pipeline.getMetadata() == null ? "<unknown>" : pipeline.getMetadata().getName();
      String yaml = pipeline.getSpec() == null ? null : pipeline.getSpec().getYaml();
      String jobKind = extractJobKind(yaml);
      // SqlJob carries dialect/executionMode as structured spec fields — prefer those over
      // name-based inference. Other job kinds (FlinkSessionJob, etc.) encode
      // the engine in the kind name itself, so inferEngine handles them.
      String engine;
      String executionMode;
      if ("SqlJob".equals(jobKind)) {
        engine = extractSqlJobField(yaml, "dialect");
        executionMode = extractSqlJobField(yaml, "executionMode");
      } else {
        engine = inferEngine(jobKind, yaml);
        executionMode = null;
      }
      return new GraphNode.Pipeline(name, jobKind, engine, executionMode);
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
      return value == null ? Collections.emptySet() : DependencyLabels.parseAnnotation(value);
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
      // sources or sinks annotation. Used to filter out hash collisions and stale labels.
      Set<String> sources = parseAnnotation(pipeline, DependencyLabels.ANNOTATION_KEY_SOURCES);
      if (sources.contains(identifier)) {
        return true;
      }
      Set<String> sinks = parseAnnotation(pipeline, DependencyLabels.ANNOTATION_KEY_SINKS);
      return sinks.contains(identifier);
    }
  }
}
