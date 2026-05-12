package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;


/**
 * Deploys a Pipeline object. Stamps {@code depends-on-*} labels and a {@code depends-on}
 * collision-guard annotation describing which sources/sink the pipeline references, so
 * {@link DependencyChecker} can look up dependents by label selector at delete time.
 *
 * <p>{@link K8sApi#update} merges labels additively, so stale {@code depends-on-*} labels from
 * a previous version of the pipeline's SQL can linger. Correctness is preserved by the
 * annotation, which is rewritten in full on every update: the checker rejects any label-only
 * match whose annotation doesn't list the target identifier. In return, we avoid the extra
 * round trip that in-place label stripping would require.
 */
class K8sPipelineDeployer extends K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList> {

  private final String name;
  private final String yaml;
  private final String sql;
  private final Collection<Source> sources;
  private final Sink sink;

  K8sPipelineDeployer(String name, List<String> specs, String sql,
      Collection<Source> sources, Sink sink, K8sContext context) {
    super(context, K8sApiEndpoints.PIPELINES);
    this.name = name;
    this.yaml = String.join("\n---\n", specs);
    this.sql = sql;
    this.sources = sources == null ? Collections.emptyList() : sources;
    this.sink = sink;
  }

  @Override
  protected V1alpha1Pipeline toK8sObject() throws SQLException {
    V1ObjectMeta meta = new V1ObjectMeta().name(name);
    DependencyLabels.stamp(meta, sources,
        sink == null ? Collections.emptyList() : Collections.singletonList(sink));
    return new V1alpha1Pipeline().kind(K8sApiEndpoints.PIPELINES.kind())
        .apiVersion(K8sApiEndpoints.PIPELINES.apiVersion())
        .metadata(meta)
        .spec(new V1alpha1PipelineSpec().sql(sql).yaml(yaml));
  }
}
