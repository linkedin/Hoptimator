package com.linkedin.hoptimator.graph.json;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.GraphRenderer;
import com.linkedin.hoptimator.graph.PipelineGraph;


/**
 * {@link GraphRenderer} that serializes a {@link PipelineGraph} as stable JSON.
 */
public final class JsonGraphRenderer implements GraphRenderer {

  public static final String FORMAT = "json";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public String format() {
    return FORMAT;
  }

  @Override
  public String render(PipelineGraph graph) {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("root", graph.root().id());
    root.put("orientation", orientation(graph));

    ArrayNode nodes = root.putArray("nodes");
    for (GraphNode node : graph.nodes()) {
      nodes.add(renderNode(node));
    }

    ArrayNode edges = root.putArray("edges");
    for (GraphEdge edge : graph.edges()) {
      edges.add(renderEdge(edge));
    }

    try {
      return MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to render pipeline graph as JSON", e);
    }
  }

  private static ObjectNode renderNode(GraphNode node) {
    ObjectNode json = MAPPER.createObjectNode();
    json.put("id", node.id());
    json.put("kind", node.kind().name());
    json.put("displayName", node.displayName());

    switch (node.kind()) {
      case PIPELINE: {
        GraphNode.Pipeline pipeline = (GraphNode.Pipeline) node;
        json.put("name", pipeline.name());
        putIfPresent(json, "jobKind", pipeline.jobKind());
        putIfPresent(json, "engine", pipeline.engine());
        putIfPresent(json, "executionMode", pipeline.executionMode());
        break;
      }
      case VIEW: {
        GraphNode.View view = (GraphNode.View) node;
        json.put("name", view.name());
        json.put("materialized", view.materialized());
        break;
      }
      case LOGICAL_TABLE: {
        GraphNode.LogicalTable logicalTable = (GraphNode.LogicalTable) node;
        json.put("name", logicalTable.name());
        ObjectNode tiers = json.putObject("tiers");
        for (Map.Entry<String, String> tier : logicalTable.tiers().entrySet()) {
          tiers.put(tier.getKey(), tier.getValue());
        }
        break;
      }
      case TRIGGER: {
        GraphNode.Trigger trigger = (GraphNode.Trigger) node;
        json.put("name", trigger.name());
        putIfPresent(json, "schedule", trigger.schedule());
        json.put("paused", trigger.paused());
        putIfPresent(json, "jobTemplateName", trigger.jobTemplateName());
        putIfPresent(json, "containerName", trigger.containerName());
        break;
      }
      case EXTERNAL: {
        GraphNode.External external = (GraphNode.External) node;
        json.put("database", external.database());
        ArrayNode path = json.putArray("path");
        for (String part : external.path()) {
          path.add(part);
        }
        break;
      }
      default:
        throw new IllegalArgumentException("Unsupported graph node kind: " + node.kind());
    }
    return json;
  }

  private static ObjectNode renderEdge(GraphEdge edge) {
    ObjectNode json = MAPPER.createObjectNode();
    json.put("from", edge.from().id());
    json.put("to", edge.to().id());
    json.put("type", edge.type().name());
    return json;
  }

  private static void putIfPresent(ObjectNode json, String fieldName, String value) {
    if (value != null) {
      json.put(fieldName, value);
    }
  }

  private static String orientation(PipelineGraph graph) {
    return graph.root().kind() == GraphNode.Kind.LOGICAL_TABLE ? "TD" : "LR";
  }
}
