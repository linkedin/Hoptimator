package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatusEstimator;
import com.linkedin.hoptimator.util.Api;

/** Provides all pipeline elements in a {@link com.linkedin.hoptimator.k8s.K8sContext} instance. */
public class K8sPipelineElementApi implements Api<K8sPipelineElement> {
  private final K8sContext context;

  K8sPipelineElementApi(K8sContext context) {
    this.context = context;
  }

  private Collection<K8sPipelineElement> discoverAllElements(K8sContext context) throws SQLException {
    final K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi = new K8sApi<>(context, K8sApiEndpoints.PIPELINES);
    Collection<V1alpha1Pipeline> pipelines = pipelineApi.list();

    Map<String, K8sPipelineElement> elements = new HashMap<>();
    K8sPipelineElementStatusEstimator statusEstimator = new K8sPipelineElementStatusEstimator(context);
    for (V1alpha1Pipeline pipeline : pipelines) {
      List<K8sPipelineElementStatus> elementStatuses = statusEstimator.estimateStatuses(pipeline);
      for (K8sPipelineElementStatus elementStatus : elementStatuses) {
        String key = elementStatus.getName();
        if (!elements.containsKey(key)) {
          elements.put(key, new K8sPipelineElement(pipeline, elementStatus));
        }
        elements.get(key).addPipeline(pipeline);
      }
    }
    return elements.values();
  }

  /**
   * Lists all pipeline elements in the context.
   */
  @Override
  public Collection<K8sPipelineElement> list() throws SQLException {
    return discoverAllElements(context);
  }
}
