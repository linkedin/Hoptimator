package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sPipelineElementTest {

  @Test
  void nameReturnsStatusName() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("my-element", true, false, "OK");
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline-1"));
    Map<String, String> configs = Collections.singletonMap("key", "value");

    K8sPipelineElement element = new K8sPipelineElement(pipeline, status, configs);

    assertEquals("my-element", element.name());
  }

  @Test
  void statusReturnsStatus() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("el", true, false, "Ready");
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline-1"));

    K8sPipelineElement element = new K8sPipelineElement(pipeline, status, Collections.emptyMap());

    assertTrue(element.status().isReady());
    assertFalse(element.status().isFailed());
  }

  @Test
  void configsReturnsConfigs() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("el", true, false, "OK");
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline-1"));
    Map<String, String> configs = Collections.singletonMap("connector", "kafka");

    K8sPipelineElement element = new K8sPipelineElement(pipeline, status, configs);

    assertEquals("kafka", element.configs().get("connector"));
  }

  @Test
  void pipelineNamesReturnsSinglePipeline() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("el", true, false, "OK");
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline-1"));

    K8sPipelineElement element = new K8sPipelineElement(pipeline, status, Collections.emptyMap());

    List<String> names = element.pipelineNames();
    assertEquals(1, names.size());
    assertEquals("pipeline-1", names.get(0));
  }

  @Test
  void addPipelineAddsNewPipeline() {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("el", true, false, "OK");
    V1alpha1Pipeline pipeline1 = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline-1"));
    V1alpha1Pipeline pipeline2 = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline-2"));

    K8sPipelineElement element = new K8sPipelineElement(pipeline1, status, Collections.emptyMap());
    element.addPipeline(pipeline2);

    List<String> names = element.pipelineNames();
    assertEquals(2, names.size());
    assertTrue(names.contains("pipeline-1"));
    assertTrue(names.contains("pipeline-2"));
  }
}
