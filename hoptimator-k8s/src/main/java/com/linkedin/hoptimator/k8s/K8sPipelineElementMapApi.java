package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.Api;
import java.sql.SQLException;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/** Provides the n:n mapping information between pipelines and their elements. */
public class K8sPipelineElementMapApi implements Api<K8sPipelineElementMapEntry> {

  private final K8sPipelineElementApi pipelineElementApi;

  public K8sPipelineElementMapApi(K8sPipelineElementApi pipelineElementApi) {
    this.pipelineElementApi = pipelineElementApi;
  }

  /**
   * Lists all n:n mapping information between pipelines and their elements.
   */
  @Override
  public Collection<K8sPipelineElementMapEntry> list() throws SQLException {
    return pipelineElementApi.list()
        .stream()
        .flatMap(K8sPipelineElementMapApi::mapEntriesFromElement)
        .collect(Collectors.toList());
  }

  private static Stream<K8sPipelineElementMapEntry> mapEntriesFromElement(K8sPipelineElement element) {
    String elementName = element.getStatus().getName();
    return element.getPipelineNames()
        .stream()
        .map(pipelineName -> new K8sPipelineElementMapEntry(elementName, pipelineName));
  }
}
