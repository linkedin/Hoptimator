package com.linkedin.hoptimator.k8s;

import java.util.LinkedList;

import org.apache.calcite.schema.impl.ViewTable;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewSpec;

import static java.util.Objects.requireNonNull;


class K8sViewDeployer extends K8sDeployer<ViewTable, V1alpha1View, V1alpha1ViewList> {

  K8sViewDeployer(K8sContext context) {
    super(context, K8sApiEndpoints.VIEWS);
  }

  @Override
  protected V1alpha1View toK8sObject(ViewTable view) {
    LinkedList<String> q = new LinkedList<>(requireNonNull(view.getViewPath()));
    String name = K8sUtils.canonicalizeName(q);
    return new V1alpha1View().kind(K8sApiEndpoints.VIEWS.kind())
        .apiVersion(K8sApiEndpoints.VIEWS.apiVersion())
        .metadata(new V1ObjectMeta().name(name))
        .spec(
            new V1alpha1ViewSpec().view(q.pollLast()).schema(q.pollLast()).sql(view.getViewSql()).materialized(false));
  }
}
