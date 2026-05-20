package com.linkedin.hoptimator.graph.mermaid;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.GraphRenderer;
import com.linkedin.hoptimator.graph.PipelineGraph;


/**
 * {@link GraphRenderer} that serializes a {@link PipelineGraph} as a Mermaid {@code flowchart}
 * string. Registered via {@code META-INF/services/com.linkedin.hoptimator.graph.GraphRenderer} so it's
 * discovered through {@link com.linkedin.hoptimator.jdbc.GraphService}.
 *
 * <p>Visual encoding:
 * <ul>
 *   <li>{@link GraphNode.External} — cylinder.</li>
 *   <li>{@link GraphNode.Pipeline} — parallelogram, with optional kind/engine inline.</li>
 *   <li>{@link GraphNode.View} — rectangle ({@code "Materialized View"} prefix when applicable).</li>
 *   <li>{@link GraphNode.LogicalTable} — top-level subgraph wrapper; tiers nest inside as subgraphs.</li>
 *   <li>{@link GraphNode.Trigger} — rhombus with cron + paused state + job hints.</li>
 * </ul>
 *
 * <p>Edges:
 * <ul>
 *   <li>{@link GraphEdge.Type#DEPENDS_ON_SOURCE}, {@link GraphEdge.Type#DEPENDS_ON_SINK} — solid arrow.</li>
 *   <li>{@link GraphEdge.Type#TRIGGERS} — dotted arrow.</li>
 *   <li>{@link GraphEdge.Type#OWNER_OF} — drives subgraph membership instead of an arrow.</li>
 * </ul>
 *
 * <p>Orientation: {@code TD} for LogicalTable graphs (with {@code direction LR} inside the LT
 * subgraph so inter-tier flows still read left-to-right); {@code LR} otherwise.
 */
public final class MermaidRenderer implements GraphRenderer {

  public static final String FORMAT = "mermaid";

  @Override
  public String format() {
    return FORMAT;
  }

  @Override
  public String render(PipelineGraph graph) {
    StringBuilder sb = new StringBuilder();
    sb.append("flowchart ").append(orientation(graph)).append("\n");

    // Stable mermaid IDs (n0, n1, ...) keyed off node identity.
    Map<GraphNode, String> mermaidIds = assignIds(graph);

    // OWNER_OF edges drive subgraph membership; collect them so we can group children inside
    // their owner.
    Map<GraphNode, Set<GraphNode>> ownedChildren = new LinkedHashMap<>();
    Set<GraphNode> ownedNodes = new LinkedHashSet<>();
    for (GraphEdge e : graph.edges()) {
      if (e.type() == GraphEdge.Type.OWNER_OF) {
        ownedChildren.computeIfAbsent(e.from(), k -> new LinkedHashSet<>()).add(e.to());
        ownedNodes.add(e.to());
      }
    }

    // Render nodes — owners get a subgraph wrapper, owned children render inside it, free
    // nodes render at the top level.
    Set<GraphNode> rendered = new LinkedHashSet<>();
    for (GraphNode node : graph.nodes()) {
      if (rendered.contains(node) || ownedNodes.contains(node)) {
        continue;
      }
      if (node.kind() == GraphNode.Kind.LOGICAL_TABLE) {
        renderLogicalTableSubgraph(sb, (GraphNode.LogicalTable) node, ownedChildren, mermaidIds, rendered, "  ");
      } else if (ownedChildren.containsKey(node)) {
        renderOwnerSubgraph(sb, node, ownedChildren, mermaidIds, rendered, "  ");
      } else {
        sb.append("  ").append(renderNode(node, mermaidIds)).append("\n");
        rendered.add(node);
      }
    }

    // Render arrows (everything except OWNER_OF).
    for (GraphEdge e : graph.edges()) {
      if (e.type() == GraphEdge.Type.OWNER_OF) {
        continue;
      }
      sb.append("  ").append(mermaidIds.get(e.from()))
          .append(arrow(e.type()))
          .append(mermaidIds.get(e.to())).append("\n");
    }
    return sb.toString();
  }

  // ─── Subgraph rendering ──────────────────────────────────────────────────

  private static void renderLogicalTableSubgraph(StringBuilder sb, GraphNode.LogicalTable lt,
      Map<GraphNode, Set<GraphNode>> ownedChildren, Map<GraphNode, String> ids,
      Set<GraphNode> rendered, String indent) {
    String ltId = ids.get(lt);
    // LT wrapper carries both kind and name — none of the inner tier subgraphs or owned
    // children show the LT name, so it has to live here. (Contrast with View wrappers: the
    // inner pipeline node duplicates the View name, so the wrapper drops the name.)
    sb.append(indent).append("subgraph ").append(ltId).append("[\"LogicalTable ")
        .append(escape(lt.displayName())).append("\"]\n");
    sb.append(indent).append("  direction LR\n");
    rendered.add(lt);

    // Group LogicalTable's owned children into tier subgraphs based on the tier-database match.
    Map<String, String> tiers = lt.tiers();
    Map<String, Set<GraphNode>> nodesByTier = new LinkedHashMap<>();
    Set<GraphNode> nonTierChildren = new LinkedHashSet<>();
    Set<GraphNode> children = ownedChildren.getOrDefault(lt, new LinkedHashSet<>());

    for (GraphNode child : children) {
      String tier = tierFor(child, tiers);
      if (tier != null) {
        nodesByTier.computeIfAbsent(tier, k -> new LinkedHashSet<>()).add(child);
      } else {
        nonTierChildren.add(child);
      }
    }

    // Emit a subgraph per tier, in the order the LogicalTable declares them.
    for (String tier : tiers.keySet()) {
      Set<GraphNode> tierNodes = nodesByTier.get(tier);
      if (tierNodes == null) {
        continue;
      }
      sb.append(indent).append("  subgraph ").append(safeId(tier))
          .append("[\"").append(escape(tier)).append("\"]\n");
      for (GraphNode n : tierNodes) {
        sb.append(indent).append("    ").append(renderNode(n, ids)).append("\n");
        rendered.add(n);
      }
      sb.append(indent).append("  end\n");
    }

    // Anything else owned by the LogicalTable (Pipelines, Triggers) — flat inside the wrapper.
    for (GraphNode n : nonTierChildren) {
      sb.append(indent).append("  ").append(renderNode(n, ids)).append("\n");
      rendered.add(n);
    }

    sb.append(indent).append("end\n");
  }

  private static void renderOwnerSubgraph(StringBuilder sb, GraphNode owner,
      Map<GraphNode, Set<GraphNode>> ownedChildren, Map<GraphNode, String> ids,
      Set<GraphNode> rendered, String indent) {
    // Mermaid rejects "Setting n0 as parent of n0" if a node id collides with the subgraph id.
    // The owner is the subgraph (its display name is the subgraph title), so we don't emit it
    // as a separate node — same convention LogicalTable already uses.
    String ownerId = ids.get(owner);
    sb.append(indent).append("subgraph ").append(ownerId).append("[\"")
        .append(escape(subgraphTitle(owner))).append("\"]\n");
    rendered.add(owner);
    for (GraphNode child : ownedChildren.get(owner)) {
      sb.append(indent).append("  ").append(renderNode(child, ids)).append("\n");
      rendered.add(child);
    }
    sb.append(indent).append("end\n");
  }

  /**
   * Subgraph title for an owner. Strips the resource name from {@link GraphNode.View}'s
   * display string ("Materialized View foo" → "Materialized View") since the owned pipeline
   * node already shows the name inside the subgraph — repeating it on the wrapper is noise.
   * Other node kinds fall through to {@link GraphNode#displayName()} unchanged.
   */
  private static String subgraphTitle(GraphNode owner) {
    if (owner instanceof GraphNode.View) {
      return ((GraphNode.View) owner).materialized() ? "Materialized View" : "View";
    }
    return owner.displayName();
  }

  private static String tierFor(GraphNode child, Map<String, String> tiers) {
    if (!(child instanceof GraphNode.External)) {
      return null;
    }
    String db = ((GraphNode.External) child).database();
    for (Map.Entry<String, String> e : tiers.entrySet()) {
      if (e.getValue().equals(db)) {
        return e.getKey();
      }
    }
    return null;
  }

  // ─── Per-node mermaid syntax ─────────────────────────────────────────────

  private static String renderNode(GraphNode node, Map<GraphNode, String> ids) {
    String id = ids.get(node);
    switch (node.kind()) {
      case EXTERNAL: {
        GraphNode.External ext = (GraphNode.External) node;
        return id + "[(\"" + ext.displayName() + "\")]";
      }
      case PIPELINE: {
        GraphNode.Pipeline p = (GraphNode.Pipeline) node;
        StringBuilder lbl = new StringBuilder(p.displayName());
        if (p.jobKind() != null) {
          lbl.append("<br/>kind: ").append(p.jobKind());
        }
        if (p.engine() != null) {
          lbl.append("<br/>engine: ").append(p.engine());
        }
        if (p.executionMode() != null) {
          lbl.append("<br/>mode: ").append(p.executionMode());
        }
        return id + "[/\"" + escape(lbl.toString()) + "\"/]";
      }
      case VIEW: {
        return id + "[\"" + escape(node.displayName()) + "\"]";
      }
      case TRIGGER: {
        GraphNode.Trigger t = (GraphNode.Trigger) node;
        StringBuilder lbl = new StringBuilder(t.displayName());
        if (t.schedule() != null) {
          lbl.append("<br/>cron: ").append(CronHumanizer.humanize(t.schedule()));
        }
        if (t.jobTemplateName() != null) {
          lbl.append("<br/>template: ").append(t.jobTemplateName());
      }
        if (t.containerName() != null) {
          lbl.append("<br/>container: ").append(t.containerName());
        }
        if (t.paused()) {
          lbl.append("<br/>(paused)");
        }
        return id + "{\"" + escape(lbl.toString()) + "\"}";
      }
      case LOGICAL_TABLE:
        // LogicalTable always renders as a subgraph wrapper, never as a flat node.
        return id + "[\"" + escape(node.displayName()) + "\"]";
      default:
        return id + "[\"" + escape(node.displayName()) + "\"]";
    }
  }

  // ─── Edge syntax ─────────────────────────────────────────────────────────

  private static String arrow(GraphEdge.Type type) {
    switch (type) {
      case TRIGGERS:
        return " -.-> ";
      case DEPENDS_ON_SOURCE:
      case DEPENDS_ON_SINK:
      default:
        return " --> ";
    }
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────

  private static String orientation(PipelineGraph graph) {
    return graph.root().kind() == GraphNode.Kind.LOGICAL_TABLE ? "TD" : "LR";
  }

  private static Map<GraphNode, String> assignIds(PipelineGraph graph) {
    Map<GraphNode, String> ids = new HashMap<>();
    int i = 0;
    for (GraphNode n : graph.nodes()) {
      ids.put(n, "n" + i++);
    }
    return ids;
  }

  private static String safeId(String raw) {
    StringBuilder sb = new StringBuilder("s_");
    for (char c : raw.toCharArray()) {
      sb.append(Character.isLetterOrDigit(c) ? c : '_');
    }
    return sb.toString();
  }

  private static String escape(String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\"", "&quot;");
  }
}
