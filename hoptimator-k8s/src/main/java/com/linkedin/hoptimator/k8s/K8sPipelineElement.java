package com.linkedin.hoptimator.k8s;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;

/** Represents a pipeline element status and its associated pipelines. */
public class K8sPipelineElement {
  private String name;
  private final K8sPipelineElementStatus status;
  private final Set<V1alpha1Pipeline> pipelines = new HashSet<>();

  public K8sPipelineElement(V1alpha1Pipeline pipeline, K8sPipelineElementStatus status) {
    this.name = status.getName();
    this.status = status;
    this.pipelines.add(pipeline);
  }

  public String name() {
    return name;
  }

  public K8sPipelineElementStatus status() {
    return status;
  }

  public void addPipeline(V1alpha1Pipeline pipeline) {
    pipelines.add(pipeline);
  }

  public List<String> pipelineNames() {
    return pipelines.stream().map(p -> Objects.requireNonNull(p.getMetadata()).getName()).collect(Collectors.toList());
  }
}
