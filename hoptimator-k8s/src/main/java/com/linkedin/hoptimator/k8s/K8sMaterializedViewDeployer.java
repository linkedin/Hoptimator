package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.MaterializedView;

import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.util.LinkedList;

class K8sMaterializedViewDeployer extends K8sDeployer<MaterializedView, V1alpha1View, V1alpha1ViewList> {

  K8sMaterializedViewDeployer(K8sContext context) {
    super(context, K8sApiEndpoints.VIEWS);
  }

  @Override
  protected V1alpha1View toK8sObject(MaterializedView view) {
    String name = K8sUtils.canonicalizeName(view.path());
    LinkedList<String> q = new LinkedList<>(view.path());
    return new V1alpha1View().kind(K8sApiEndpoints.VIEWS.kind())
        .apiVersion(K8sApiEndpoints.VIEWS.apiVersion())
        .metadata(new V1ObjectMeta().name(name))
        .spec(new V1alpha1ViewSpec()
            .view(q.pollLast()).schema(q.pollLast())
            .sql(view.sql()).materialized(true));
  }
}
