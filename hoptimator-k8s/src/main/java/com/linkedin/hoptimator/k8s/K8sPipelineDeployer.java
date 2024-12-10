package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.stream.Collectors;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;


/** Deploys the Pipeline behind a MaterializedView. */
class K8sPipelineDeployer extends K8sDeployer<MaterializedView, V1alpha1Pipeline, V1alpha1PipelineList> {

  K8sPipelineDeployer(K8sContext context) {
    super(context, K8sApiEndpoints.PIPELINES);
  }

  @Override
  protected V1alpha1Pipeline toK8sObject(MaterializedView view) throws SQLException {
    String name = K8sUtils.canonicalizeName(view.path());
    String yaml = view.pipeline().specify().stream().collect(Collectors.joining("\n---\n"));
    String sql = view.pipelineSql().apply(SqlDialect.ANSI);
    return new V1alpha1Pipeline().kind(K8sApiEndpoints.PIPELINES.kind())
        .apiVersion(K8sApiEndpoints.PIPELINES.apiVersion())
        .metadata(new V1ObjectMeta().name(name))
        .spec(new V1alpha1PipelineSpec().sql(sql).yaml(yaml));
  }
}
