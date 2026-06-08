package com.linkedin.hoptimator.graph.json;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.graph.GraphEdge;
import com.linkedin.hoptimator.graph.GraphNode;
import com.linkedin.hoptimator.graph.PipelineGraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Unit tests for {@link JsonGraphRenderer}. These exercise the JSON contract in isolation
 * (hand-built {@link PipelineGraph} fixtures, no K8s involvement).
 */
class JsonGraphRendererTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void rendersAllNodeKindsAndIncludesOwnerEdges() throws Exception {
    GraphNode.View root = new GraphNode.View("audience", true);
    GraphNode.Pipeline pipeline = new GraphNode.Pipeline("audience-pipe",
        "FlinkDeployment", "Flink", "Streaming");
    GraphNode.Trigger trigger = new GraphNode.Trigger("audience-trigger",
        "0 */6 * * *", true, "retl-job-template", "main");
    GraphNode.External external = new GraphNode.External("kafka-db",
        Arrays.asList("KAFKA", "events"));
    Map<String, String> tiers = new LinkedHashMap<>();
    tiers.put("nearline", "kafka-db");
    GraphNode.LogicalTable logicalTable = new GraphNode.LogicalTable("audience-lt", tiers);

    Set<GraphNode> nodes = setOf(root, pipeline, trigger, external, logicalTable);
    Set<GraphEdge> edges = setOf(
        new GraphEdge(root, pipeline, GraphEdge.Type.OWNER_OF),
        new GraphEdge(trigger, pipeline, GraphEdge.Type.TRIGGERS),
        new GraphEdge(external, pipeline, GraphEdge.Type.DEPENDS_ON_SOURCE));

    JsonNode json = render(new PipelineGraph(root, nodes, edges));

    assertEquals("json", new JsonGraphRenderer().format());
    assertEquals(root.id(), json.get("root").asText());
    assertEquals("LR", json.get("orientation").asText());
    assertEquals(root.id(), json.get("nodes").get(0).get("id").asText());
    assertEquals("VIEW", node(json, root).get("kind").asText());
    assertEquals("PIPELINE", node(json, pipeline).get("kind").asText());
    assertEquals("TRIGGER", node(json, trigger).get("kind").asText());
    assertEquals("EXTERNAL", node(json, external).get("kind").asText());
    assertEquals("LOGICAL_TABLE", node(json, logicalTable).get("kind").asText());

    JsonNode pipelineJson = node(json, pipeline);
    assertEquals("audience-pipe", pipelineJson.get("displayName").asText());
    assertEquals("audience-pipe", pipelineJson.get("name").asText());
    assertEquals("FlinkDeployment", pipelineJson.get("jobKind").asText());
    assertEquals("Flink", pipelineJson.get("engine").asText());
    assertEquals("Streaming", pipelineJson.get("executionMode").asText());

    JsonNode triggerJson = node(json, trigger);
    assertEquals("0 */6 * * *", triggerJson.get("schedule").asText());
    assertTrue(triggerJson.get("paused").asBoolean());
    assertEquals("retl-job-template", triggerJson.get("jobTemplateName").asText());
    assertEquals("main", triggerJson.get("containerName").asText());

    JsonNode externalJson = node(json, external);
    assertEquals("kafka-db", externalJson.get("database").asText());
    assertEquals("KAFKA", externalJson.get("path").get(0).asText());
    assertEquals("events", externalJson.get("path").get(1).asText());

    JsonNode ownerEdge = edge(json, root, pipeline, GraphEdge.Type.OWNER_OF);
    assertNotNull(ownerEdge, "OWNER_OF edges must be included in JSON output");
    assertEquals("TRIGGERS", json.get("edges").get(1).get("type").asText());
  }

  @Test
  void logicalTableRootUsesTdOrientationAndPreservesTierOrder() throws Exception {
    Map<String, String> tiers = new LinkedHashMap<>();
    tiers.put("nearline", "kafka-db");
    tiers.put("online", "venice-db");
    tiers.put("offline", "hdfs-db");
    GraphNode.LogicalTable root = new GraphNode.LogicalTable("foo", tiers);
    Set<GraphNode> nodes = setOf(root);
    Set<GraphEdge> edges = setOf();

    JsonNode json = render(new PipelineGraph(root, nodes, edges));

    assertEquals("TD", json.get("orientation").asText());
    JsonNode tiersJson = node(json, root).get("tiers");
    assertEquals("kafka-db", tiersJson.get("nearline").asText());
    assertEquals("venice-db", tiersJson.get("online").asText());
    assertEquals("hdfs-db", tiersJson.get("offline").asText());

    Iterator<String> fieldNames = tiersJson.fieldNames();
    assertEquals("nearline", fieldNames.next());
    assertEquals("online", fieldNames.next());
    assertEquals("offline", fieldNames.next());
    assertFalse(fieldNames.hasNext());
  }

  @Test
  void nullableFieldsAreOmittedButBooleansAreAlwaysIncluded() throws Exception {
    GraphNode.Pipeline pipeline = new GraphNode.Pipeline("p1", null, null, null);
    GraphNode.Trigger trigger = new GraphNode.Trigger("t1", null, false, null, null);
    GraphNode.View view = new GraphNode.View("v1", false);
    Set<GraphNode> nodes = setOf(pipeline, trigger, view);
    Set<GraphEdge> edges = setOf();

    JsonNode json = render(new PipelineGraph(pipeline, nodes, edges));

    assertEquals("LR", json.get("orientation").asText());
    JsonNode pipelineJson = node(json, pipeline);
    assertFalse(pipelineJson.has("jobKind"));
    assertFalse(pipelineJson.has("engine"));
    assertFalse(pipelineJson.has("executionMode"));

    JsonNode triggerJson = node(json, trigger);
    assertFalse(triggerJson.has("schedule"));
    assertFalse(triggerJson.get("paused").asBoolean());
    assertFalse(triggerJson.has("jobTemplateName"));
    assertFalse(triggerJson.has("containerName"));

    JsonNode viewJson = node(json, view);
    assertTrue(viewJson.has("materialized"));
    assertFalse(viewJson.get("materialized").asBoolean());
  }

  private static JsonNode render(PipelineGraph graph) throws Exception {
    return MAPPER.readTree(new JsonGraphRenderer().render(graph));
  }

  private static JsonNode node(JsonNode graph, GraphNode node) {
    for (JsonNode n : graph.get("nodes")) {
      if (node.id().equals(n.get("id").asText())) {
        return n;
      }
    }
    throw new AssertionError("Missing node " + node.id() + " in " + graph);
  }

  private static JsonNode edge(JsonNode graph, GraphNode from, GraphNode to, GraphEdge.Type type) {
    for (JsonNode edge : graph.get("edges")) {
      if (from.id().equals(edge.get("from").asText())
          && to.id().equals(edge.get("to").asText())
          && type.name().equals(edge.get("type").asText())) {
        return edge;
      }
    }
    return null;
  }

  @SafeVarargs
  private static <T> Set<T> setOf(T... items) {
    Set<T> set = new LinkedHashSet<>();
    Collections.addAll(set, items);
    return set;
  }
}
