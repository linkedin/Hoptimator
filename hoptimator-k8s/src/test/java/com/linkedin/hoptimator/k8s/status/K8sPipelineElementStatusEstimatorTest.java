package com.linkedin.hoptimator.k8s.status;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;

import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
public class K8sPipelineElementStatusEstimatorTest {
  @Mock
  private K8sContext context;
  private K8sPipelineElementStatusEstimator estimator;
  @Mock
  private V1alpha1Pipeline pipeline;
  @Mock
  private V1ObjectMeta pipelineMetadata;
  @Mock
  private V1alpha1PipelineSpec pipelineSpec;
  @Mock
  private DynamicKubernetesApi dynamicKubernetesApi;
  @Mock
  KubernetesApiResponse<DynamicKubernetesObject> jobDynamicKubernetesApiResponse;
  @Mock
  KubernetesApiResponse<DynamicKubernetesObject> kafkaDynamicKubernetesApiResponse;

  @Mock
  DynamicKubernetesObject jobDynamicObject;

  @Mock
  JsonObject jobDynamicObjectRawJsonObject;

  @Mock
  JsonElement jobDynamicObjectStatusJsonElement;

  @Mock
  JsonObject jobDynamicObjectStatusJsonObject;

  private static final String FAKE_JOB_SPEC =
      "apiVersion: foo.org/v1beta1\n" + "kind: FakeJob\n" + "metadata:\n" + "  name: fake-job-name\n" + "spec:\n"
          + "  deploymentName: fake-deployment\n" + "  job:\n" + "    entryClass: com.runner.FakeRunner";
  private static final String FAKE_KAFKA_TOPIC_SPEC =
      "apiVersion: kafka.strimzi.io/v1beta2\n" + "kind: KafkaTopic\n" + "metadata:\n" + "      name: fake-kafka-topic\n"
          + "      labels:\n" + "        strimzi.io/cluster: one\n" + "spec:\n" + "      topicName: existing-topic-1\n"
          + "      partitions: 1";

  private static final String FAKE_MULTIPLE_SPECS = FAKE_JOB_SPEC + "\n---\n"

      + FAKE_KAFKA_TOPIC_SPEC + "\n";

  private static final Set<String> READY_STRINGS = ImmutableSet.of("READY", "RUNNING", "FINISHED");
  private static final Set<String> FAILED_STRINGS = ImmutableSet.of("CRASHLOOPBACKOFF", "FAILED");

  @BeforeEach
  void setUp() {
    estimator = new K8sPipelineElementStatusEstimator(context);
    when(pipelineMetadata.getNamespace()).thenReturn("fake-namespace");
    when(pipeline.getMetadata()).thenReturn(pipelineMetadata);
    when(pipeline.getSpec()).thenReturn(pipelineSpec);
    when(pipelineSpec.getYaml()).thenReturn(FAKE_JOB_SPEC);
    when(context.dynamic(any(), any())).thenReturn(dynamicKubernetesApi);
    when(dynamicKubernetesApi.get(anyString(), anyString())).thenReturn(jobDynamicKubernetesApiResponse);
    when(jobDynamicKubernetesApiResponse.isSuccess()).thenReturn(true);
  }

  @Test
  void testEstimateWhenPipelineHasMultipleElements() {
    when(pipelineSpec.getYaml()).thenReturn(FAKE_MULTIPLE_SPECS);
    // Set up: failed to get kafka object from K8s.
    when(kafkaDynamicKubernetesApiResponse.isSuccess()).thenReturn(false);
    // Set up: successfully get job object from K8s, and it contains status field as ready.
    mockJobDynamicObjectWithStatusField();
    JsonElement readyElement = mock(JsonElement.class);
    when(readyElement.getAsBoolean()).thenReturn(true);
    when(jobDynamicObjectStatusJsonObject.get("ready")).thenReturn(readyElement);
    when(jobDynamicObjectStatusJsonObject.has("failed")).thenReturn(false);
    when(jobDynamicObjectStatusJsonObject.has("message")).thenReturn(false);
    when(dynamicKubernetesApi.get(anyString(), anyString())).thenReturn(jobDynamicKubernetesApiResponse)
        .thenReturn(kafkaDynamicKubernetesApiResponse);

    List<K8sPipelineElementStatus> statuses = estimator.estimateStatuses(pipeline);
    assertEquals(2, statuses.size());
    K8sPipelineElementStatus jobStatus = statuses.get(0);
    assertTrue(jobStatus.isReady());
    assertFalse(jobStatus.isFailed());
    assertEquals("", jobStatus.getMessage());

    K8sPipelineElementStatus kafkaStatus = statuses.get(1);
    assertFalse(kafkaStatus.isReady());
    assertFalse(kafkaStatus.isFailed());
    assertTrue(
        kafkaStatus.getMessage().startsWith("Failed to fetch KafkaTopic/fake-kafka-topic in namespace fake-namespace"));
  }

  @Test
  void testEstimateWhenPipelineHasSingleElementWithK8sObjectHavingStatusFieldWithReadyInfo() {
    mockJobDynamicObjectWithStatusField();
    JsonElement readyElement = mock(JsonElement.class);
    when(readyElement.getAsBoolean()).thenReturn(true);
    when(jobDynamicObjectStatusJsonObject.get("ready")).thenReturn(readyElement);
    when(jobDynamicObjectStatusJsonObject.has("failed")).thenReturn(false);

    JsonElement messageElement = mock(JsonElement.class);
    when(messageElement.getAsString()).thenReturn("fake-message");
    when(jobDynamicObjectStatusJsonObject.has("message")).thenReturn(true);
    when(jobDynamicObjectStatusJsonObject.get("message")).thenReturn(messageElement);

    List<K8sPipelineElementStatus> statuses = estimator.estimateStatuses(pipeline);
    K8sPipelineElementStatus status = Iterables.getOnlyElement(statuses);
    assertTrue(status.isReady());
    assertFalse(status.isFailed());
    assertEquals("fake-message", status.getMessage());
  }

  private void mockJobDynamicObjectWithStatusField() {
    when(jobDynamicObjectRawJsonObject.has("status")).thenReturn(true);
    when(jobDynamicObjectRawJsonObject.get("status")).thenReturn(jobDynamicObjectStatusJsonElement);
    when(jobDynamicObjectStatusJsonElement.getAsJsonObject()).thenReturn(jobDynamicObjectStatusJsonObject);
    when(jobDynamicKubernetesApiResponse.getObject()).thenReturn(jobDynamicObject);
    when(jobDynamicObject.getRaw()).thenReturn(jobDynamicObjectRawJsonObject);
    when(jobDynamicKubernetesApiResponse.getObject()).thenReturn(jobDynamicObject);
  }

  @ParameterizedTest
  @FieldSource("READY_STRINGS")
  @FieldSource("FAILED_STRINGS")
  void testEstimateWhenPipelineHasSingleElementWithK8sObjectHavingStatusFieldWithStateInfo(String state) {
    mockJobDynamicObjectWithStatusField();
    JsonElement stateElement = mock(JsonElement.class);
    when(stateElement.getAsString()).thenReturn(state);
    when(jobDynamicObjectStatusJsonObject.get("ready")).thenReturn(null);
    when(jobDynamicObjectStatusJsonObject.get("state")).thenReturn(stateElement);

    List<K8sPipelineElementStatus> statuses = estimator.estimateStatuses(pipeline);
    K8sPipelineElementStatus status = Iterables.getOnlyElement(statuses);
    assertEquals(READY_STRINGS.contains(state), status.isReady());
    assertEquals(FAILED_STRINGS.contains(state), status.isFailed());
    assertEquals(state, status.getMessage());
  }

  @ParameterizedTest
  @FieldSource("READY_STRINGS")
  @FieldSource("FAILED_STRINGS")
  void testEstimateWhenPipelineHasSingleElementWithK8sObjectHavingStatusFieldWithJobStatusStateInfo(String state) {
    mockJobDynamicObjectWithStatusField();
    JsonElement stateElement = mock(JsonElement.class);
    when(stateElement.getAsString()).thenReturn(state);
    JsonObject jobStatusJsonObject = mock(JsonObject.class);
    when(jobStatusJsonObject.get("state")).thenReturn(stateElement);
    JsonElement jobStatusJsonElement = mock(JsonElement.class);
    when(jobStatusJsonElement.getAsJsonObject()).thenReturn(jobStatusJsonObject);
    when(jobDynamicObjectStatusJsonObject.get("ready")).thenReturn(null);
    when(jobDynamicObjectStatusJsonObject.get("state")).thenReturn(null);
    when(jobDynamicObjectStatusJsonObject.get("jobStatus")).thenReturn(jobStatusJsonElement);

    List<K8sPipelineElementStatus> statuses = estimator.estimateStatuses(pipeline);
    K8sPipelineElementStatus status = Iterables.getOnlyElement(statuses);
    assertEquals(READY_STRINGS.contains(state), status.isReady());
    assertEquals(FAILED_STRINGS.contains(state), status.isFailed());
    assertEquals(state, status.getMessage());
  }

  @Test
  void testEstimateWhenPipelineHasSingleElementWithK8sObjectHavingNoStatusField() {
    when(jobDynamicObjectRawJsonObject.has("status")).thenReturn(false);
    mockDynamicObjectWithMetadata();

    List<K8sPipelineElementStatus> statuses = estimator.estimateStatuses(pipeline);
    K8sPipelineElementStatus status = Iterables.getOnlyElement(statuses);
    assertTrue(status.isReady());
    assertFalse(status.isFailed());
    assertEquals("Object fake-namespace/fake-kind/fake-job-name considered ready by default.", status.getMessage());
  }

  private void mockDynamicObjectWithMetadata() {
    when(jobDynamicObject.getRaw()).thenReturn(jobDynamicObjectRawJsonObject);
    V1ObjectMeta metadata = mock(V1ObjectMeta.class);
    when(metadata.getName()).thenReturn("fake-job-name");
    when(metadata.getNamespace()).thenReturn("fake-namespace");
    when(jobDynamicObject.getMetadata()).thenReturn(metadata);
    when(jobDynamicObject.getKind()).thenReturn("fake-kind");
    when(jobDynamicKubernetesApiResponse.getObject()).thenReturn(jobDynamicObject);
  }

  @Test
  void testEstimateWhenCallToK8sFails() {
    when(jobDynamicKubernetesApiResponse.isSuccess()).thenReturn(false);
    List<K8sPipelineElementStatus> statuses = estimator.estimateStatuses(pipeline);
    K8sPipelineElementStatus status = Iterables.getOnlyElement(statuses);
    assertFalse(status.isReady());
    assertFalse(status.isFailed());
    assertTrue(status.getMessage().startsWith("Failed to fetch FakeJob/fake-job-name"));
  }

  @Test
  void testEstimateWhenK8sReturnsNullObject() {
    when(jobDynamicKubernetesApiResponse.getObject()).thenReturn(null);
    List<K8sPipelineElementStatus> statuses = estimator.estimateStatuses(pipeline);
    K8sPipelineElementStatus status = Iterables.getOnlyElement(statuses);
    assertFalse(status.isReady());
    assertFalse(status.isFailed());
    assertEquals(status.getMessage(), "Returned K8s object is null or has no json");
  }

  @Test
  void testEstimateWhenPipelineHasSingleElementWithK8sObjectHavingNoRawJson() {
    when(jobDynamicKubernetesApiResponse.getObject()).thenReturn(jobDynamicObject);
    when(jobDynamicObject.getRaw()).thenReturn(null);
    List<K8sPipelineElementStatus> statuses = estimator.estimateStatuses(pipeline);
    K8sPipelineElementStatus status = Iterables.getOnlyElement(statuses);
    assertFalse(status.isReady());
    assertFalse(status.isFailed());
    assertEquals(status.getMessage(), "Returned K8s object is null or has no json");
  }
}
