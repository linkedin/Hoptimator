package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;

import org.apache.calcite.schema.impl.ViewTable;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.util.LinkedList;

class K8sViewDeployer extends K8sDeployer<ViewTable, V1alpha1View, V1alpha1ViewList> {

  K8sViewDeployer(K8sContext context) {
    super(context, K8sApiEndpoints.VIEWS);
  }

  @Override
  protected V1alpha1View toK8sObject(ViewTable view) {
    String name = K8sUtils.canonicalizeName(view.getViewPath());
    LinkedList<String> q = new LinkedList<>(view.getViewPath());
    return new V1alpha1View().kind(K8sApiEndpoints.VIEWS.kind())
        .apiVersion(K8sApiEndpoints.VIEWS.apiVersion())
        .metadata(new V1ObjectMeta().name(name))
        .spec(new V1alpha1ViewSpec()
            .view(q.pollLast()).schema(q.pollLast())
            .sql(view.getViewSql()).materialized(false));
  }
}
