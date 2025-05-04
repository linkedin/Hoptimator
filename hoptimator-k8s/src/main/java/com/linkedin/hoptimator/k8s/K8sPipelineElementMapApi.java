package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.Api;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class K8sPipelineElementMapApi implements Api<K8sPipelineElementMapEntry> {

  private final K8sPipelineElementApi pipelineElementApi;

  public K8sPipelineElementMapApi(K8sPipelineElementApi pipelineElementApi) {
    this.pipelineElementApi = pipelineElementApi;
  }

  /**
   *
   * @return
   * @throws SQLException
   */
  @Override
  public Collection<K8sPipelineElementMapEntry> list() throws SQLException {
    return pipelineElementApi.list()
        .stream()
        .flatMap(K8sPipelineElementMapApi::mapEntriesFromElement)
        .collect(Collectors.toList());
  }

  private static Stream<K8sPipelineElementMapEntry> mapEntriesFromElement(K8sDynamicPipelineElement element) {
    String elementName = element.getStatus().getName();
    return element.getPipelines()
        .stream()
        .map(pipeline -> new K8sPipelineElementMapEntry(elementName, pipeline.getMetadata().getName()));
  }
}
