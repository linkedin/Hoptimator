package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;
import java.util.HashSet;
import java.util.Set;


public class K8sDynamicPipelineElement {
  private final K8sPipelineElementStatus status;
  private final Set<V1alpha1Pipeline> pipelines = new HashSet<>();

  public K8sDynamicPipelineElement(K8sPipelineElementStatus status) {
    this.status = status;
  }

  public K8sPipelineElementStatus getStatus() {
    return status;
  }

  public Set<V1alpha1Pipeline> getPipelines() {
    return pipelines;
  }

  public static String getKey(String namespace, String name) {
    return namespace + "/" + name;
  }
}
