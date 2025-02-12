package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;


/** Deploys a Pipeline object. */
class K8sPipelineDeployer extends K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList> {

  private final String name;
  private final String yaml;
  private final String sql;

  K8sPipelineDeployer(String name, List<String> specs, String sql, K8sContext context) {
    super(context, K8sApiEndpoints.PIPELINES);
    this.name = name;
    this.yaml = specs.stream().collect(Collectors.joining("\n---\n"));
    this.sql = sql;
  }

  @Override
  protected V1alpha1Pipeline toK8sObject() throws SQLException {
    return new V1alpha1Pipeline().kind(K8sApiEndpoints.PIPELINES.kind())
        .apiVersion(K8sApiEndpoints.PIPELINES.apiVersion())
        .metadata(new V1ObjectMeta().name(name))
        .spec(new V1alpha1PipelineSpec().sql(sql).yaml(yaml));
  }
}
