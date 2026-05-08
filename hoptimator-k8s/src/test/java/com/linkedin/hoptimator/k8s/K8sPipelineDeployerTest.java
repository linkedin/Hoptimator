package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sPipelineDeployerTest {

  private static Source src(String db, String path) {
    return new Source(db, Collections.singletonList(path), Collections.emptyMap());
  }

  private static Sink sink(String db, String path) {
    return new Sink(db, Collections.singletonList(path), Collections.emptyMap());
  }

  @Test
  void toK8sObjectSetsPipelineFields() throws SQLException {
    K8sPipelineDeployer deployer = new K8sPipelineDeployer("my-pipeline", Arrays.asList("spec1", "spec2"),
        "SELECT 1", Collections.emptyList(), null, null);

    V1alpha1Pipeline pipeline = deployer.toK8sObject();

    assertEquals("my-pipeline", pipeline.getMetadata().getName());
    assertEquals("SELECT 1", pipeline.getSpec().getSql());
    assertEquals("spec1\n---\nspec2", pipeline.getSpec().getYaml());
    assertEquals("Pipeline", pipeline.getKind());
    assertNotNull(pipeline.getApiVersion());
  }

  @Test
  void toK8sObjectWithSingleSpec() throws SQLException {
    K8sPipelineDeployer deployer = new K8sPipelineDeployer("single", List.of("only-spec"),
        "SELECT 1", Collections.emptyList(), null, null);

    V1alpha1Pipeline pipeline = deployer.toK8sObject();

    assertEquals("only-spec", pipeline.getSpec().getYaml());
  }

  @Test
  void stampsDependencyLabelsForSourcesAndSink() throws SQLException {
    K8sPipelineDeployer deployer = new K8sPipelineDeployer(
        "p1", List.of("spec"), "SELECT 1",
        Arrays.asList(src("kafka1", "topic-a"), src("kafka2", "topic-b")),
        sink("mysql", "outbox"), null);

    V1alpha1Pipeline pipeline = deployer.toK8sObject();
    Map<String, String> labels = pipeline.getMetadata().getLabels();

    assertEquals(3, labels.size(), "should have one label per source + one for the sink");
    assertTrue(labels.containsKey(
        PipelineDependencyLabels.labelKey("kafka1", Collections.singletonList("topic-a"))));
    assertTrue(labels.containsKey(
        PipelineDependencyLabels.labelKey("kafka2", Collections.singletonList("topic-b"))));
    assertTrue(labels.containsKey(
        PipelineDependencyLabels.labelKey("mysql", Collections.singletonList("outbox"))));
  }

  @Test
  void stampsDirectionalAnnotations() throws SQLException {
    K8sPipelineDeployer deployer = new K8sPipelineDeployer(
        "p1", List.of("spec"), "SELECT 1",
        Collections.singletonList(src("kafka", "topic")),
        sink("mysql", "outbox"), null);

    V1alpha1Pipeline pipeline = deployer.toK8sObject();
    Map<String, String> annotations = pipeline.getMetadata().getAnnotations();

    String sources = annotations.get(PipelineDependencyLabels.ANNOTATION_KEY_SOURCES);
    String sink = annotations.get(PipelineDependencyLabels.ANNOTATION_KEY_SINK);
    assertNotNull(sources);
    assertNotNull(sink);
    assertTrue(sources.contains("kafka_topic"),
        "sources annotation should list the kafka source: " + sources);
    assertEquals("mysql_outbox", sink, "sink annotation holds the single identifier verbatim");
  }
}
