package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatusEstimator;
import com.linkedin.hoptimator.util.Api;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class K8sPipelineElementApi implements Api<K8sDynamicPipelineElement> {
  private final static Logger log = LoggerFactory.getLogger(K8sPipelineElementApi.class);
  private final K8sContext context;

  public K8sPipelineElementApi(K8sContext context) {
    this.context = context;
  }

  private Collection<K8sDynamicPipelineElement> discoverAllElements(K8sContext context) {
    final K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi = new K8sApi<>(context, K8sApiEndpoints.PIPELINES);
    Collection<V1alpha1Pipeline> pipelines;
    try {
      pipelines = pipelineApi.list();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    Map<String, K8sDynamicPipelineElement> elements = new HashMap<>();
    K8sPipelineElementStatusEstimator statusEstimator = new K8sPipelineElementStatusEstimator(context);
    for (V1alpha1Pipeline pipeline : pipelines) {
      String namespace = pipeline.getMetadata().getNamespace();
      List<K8sPipelineElementStatus> elementStatuses = statusEstimator.statuses(pipeline).collect(Collectors.toList());
      for (K8sPipelineElementStatus elementStatus: elementStatuses) {
        String key = K8sDynamicPipelineElement.getKey(namespace, elementStatus.getName());
        if (!elements.containsKey(key)) {
          elements.put(key, new K8sDynamicPipelineElement(elementStatus));
        }
        elements.get(key).getPipelines().add(pipeline);
      }
    }
    return elements.values();
  }

  /**
   *
   * @return
   * @throws SQLException
   */
  @Override
  public Collection<K8sDynamicPipelineElement> list() throws SQLException {
    return discoverAllElements(context);
  }
}
