package com.linkedin.hoptimator.k8s;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sPipelineDependencyValidatorTest {

  /**
   * Sentinel non-null connection to unblock the validator's null-check. The test override of
   * {@code pipelineApi(connection)} ignores the actual value, so any non-null reference works.
   */
  private static final Connection FAKE_CONNECTION = org.mockito.Mockito.mock(Connection.class);

  /** Builds a validator backed by an in-memory pipeline list. */
  private K8sPipelineDependencyValidator makeValidator(Source source, List<V1alpha1Pipeline> pipelines) {
    FakeK8sApi<V1alpha1Pipeline, V1alpha1PipelineList> api = new FakeK8sApi<>(pipelines);
    return new K8sPipelineDependencyValidator(source) {
      @Override
      K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi(Connection connection) {
        return api;
      }
    };
  }

  private static V1alpha1Pipeline pipeline(String name, String sql) {
    return new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name(name))
        .spec(new V1alpha1PipelineSpec().sql(sql));
  }

  private static Source source(String schema, String table) {
    return new Source("kafka-database", Arrays.asList(schema, table), Collections.emptyMap());
  }

  @Test
  void noPipelinesIsValid() {
    K8sPipelineDependencyValidator v = makeValidator(source("KAFKA", "topic-a"), new ArrayList<>());
    Validator.Issues issues = new Validator.Issues("");
    v.validate(issues, FAKE_CONNECTION);
    assertTrue(issues.valid());
  }

  @Test
  void unrelatedPipelineIsValid() {
    List<V1alpha1Pipeline> pipelines = new ArrayList<>();
    pipelines.add(pipeline("p1",
        "CREATE TABLE IF NOT EXISTS `KAFKA`.`other-topic` (...);\n"
        + "INSERT INTO `KAFKA`.`other-sink` SELECT * FROM `KAFKA`.`other-topic`"));

    K8sPipelineDependencyValidator v = makeValidator(source("KAFKA", "topic-a"), pipelines);
    Validator.Issues issues = new Validator.Issues("");
    v.validate(issues, FAKE_CONNECTION);
    assertTrue(issues.valid());
  }

  @Test
  void pipelineReferencingSourceProducesError() {
    List<V1alpha1Pipeline> pipelines = new ArrayList<>();
    pipelines.add(pipeline("kafka-existing-topic-1-guard",
        "CREATE TABLE IF NOT EXISTS `KAFKA`.`existing-topic-2` (...);\n"
        + "INSERT INTO `KAFKA`.`existing-topic-1` SELECT * FROM `KAFKA`.`existing-topic-2`"));

    K8sPipelineDependencyValidator v = makeValidator(source("KAFKA", "existing-topic-2"), pipelines);
    Validator.Issues issues = new Validator.Issues("");
    v.validate(issues, FAKE_CONNECTION);
    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("kafka-existing-topic-1-guard"),
        "error must name the dependent pipeline");
  }

  @Test
  void sinkReferenceAlsoBlocks() {
    // Drop on the SINK of an active pipeline should block — orphaning the pipeline's writer.
    List<V1alpha1Pipeline> pipelines = new ArrayList<>();
    pipelines.add(pipeline("kafka-existing-topic-1-guard",
        "INSERT INTO `KAFKA`.`existing-topic-1` SELECT * FROM `KAFKA`.`existing-topic-2`"));

    K8sPipelineDependencyValidator v = makeValidator(source("KAFKA", "existing-topic-1"), pipelines);
    Validator.Issues issues = new Validator.Issues("");
    v.validate(issues, FAKE_CONNECTION);
    assertFalse(issues.valid());
  }

  @Test
  void substringNameDoesNotFalseMatch() {
    // Dependency check must be exact: source `topic` should not match `topic-derived`.
    // Backtick boundaries make this naturally robust.
    List<V1alpha1Pipeline> pipelines = new ArrayList<>();
    pipelines.add(pipeline("p",
        "INSERT INTO `KAFKA`.`topic-derived-sink` SELECT * FROM `KAFKA`.`topic-derived-source`"));

    K8sPipelineDependencyValidator v = makeValidator(source("KAFKA", "topic"), pipelines);
    Validator.Issues issues = new Validator.Issues("");
    v.validate(issues, FAKE_CONNECTION);
    assertTrue(issues.valid(), "exact-name source must not match a substring-bearing pipeline");
  }

  @Test
  void multipleBlockersAllReported() {
    List<V1alpha1Pipeline> pipelines = new ArrayList<>();
    pipelines.add(pipeline("p1",
        "INSERT INTO `KAFKA`.`x` SELECT * FROM `KAFKA`.`shared-source`"));
    pipelines.add(pipeline("p2",
        "INSERT INTO `KAFKA`.`y` SELECT * FROM `KAFKA`.`shared-source`"));

    K8sPipelineDependencyValidator v = makeValidator(source("KAFKA", "shared-source"), pipelines);
    Validator.Issues issues = new Validator.Issues("");
    v.validate(issues, FAKE_CONNECTION);
    assertFalse(issues.valid());
    String msg = issues.toString();
    assertTrue(msg.contains("p1") && msg.contains("p2"),
        "both blocking pipelines must be named in the error");
  }

  @Test
  void singleElementPathHasNoPattern() {
    // Sources without a schema component (single-element path) can't be referenced
    // by a pipeline's SQL in the schema.table shape, so referencePattern returns null.
    Source flat = new Source("db", Collections.singletonList("table"), Collections.emptyMap());
    assertNull(K8sPipelineDependencyValidator.referencePattern(flat));
  }

  @Test
  void referencePatternFormatsCorrectly() {
    assertEquals("`KAFKA`.`existing-topic-2`",
        K8sPipelineDependencyValidator.referencePattern(source("KAFKA", "existing-topic-2")));
  }
}
