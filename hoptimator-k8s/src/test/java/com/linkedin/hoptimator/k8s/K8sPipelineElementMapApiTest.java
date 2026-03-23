package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class K8sPipelineElementMapApiTest {

  @Mock
  private K8sPipelineElementApi pipelineElementApi;

  private K8sPipelineElementMapApi mapApi;

  @BeforeEach
  void setUp() {
    mapApi = new K8sPipelineElementMapApi(pipelineElementApi);
  }

  @Test
  void listReturnsEmptyWhenNoElements() throws SQLException {
    when(pipelineElementApi.list()).thenReturn(Collections.emptyList());
    Collection<K8sPipelineElementMapEntry> entries = mapApi.list();
    assertTrue(entries.isEmpty());
  }

  @Test
  void listReturnsMappingsForSinglePipeline() throws SQLException {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("element-1", true, false, "OK");
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline-1"));
    K8sPipelineElement element = new K8sPipelineElement(pipeline, status, Collections.emptyMap());

    List<K8sPipelineElement> elements = new ArrayList<>();
    elements.add(element);
    when(pipelineElementApi.list()).thenReturn(elements);

    Collection<K8sPipelineElementMapEntry> entries = mapApi.list();
    assertEquals(1, entries.size());
    K8sPipelineElementMapEntry entry = entries.iterator().next();
    assertEquals("element-1", entry.elementName());
    assertEquals("pipeline-1", entry.pipelineName());
  }

  @Test
  void listReturnsMappingsForMultiplePipelines() throws SQLException {
    K8sPipelineElementStatus status = new K8sPipelineElementStatus("element-1", true, false, "OK");
    V1alpha1Pipeline pipeline1 = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline-1"));
    V1alpha1Pipeline pipeline2 = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pipeline-2"));
    K8sPipelineElement element = new K8sPipelineElement(pipeline1, status, Collections.emptyMap());
    element.addPipeline(pipeline2);

    List<K8sPipelineElement> elements = new ArrayList<>();
    elements.add(element);
    when(pipelineElementApi.list()).thenReturn(elements);

    Collection<K8sPipelineElementMapEntry> entries = mapApi.list();
    assertEquals(2, entries.size());
  }
}
