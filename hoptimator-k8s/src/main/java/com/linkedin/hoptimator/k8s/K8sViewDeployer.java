package com.linkedin.hoptimator.k8s;

import java.util.LinkedList;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.View;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewSpec;


/** Deploys a View object. */
class K8sViewDeployer extends K8sDeployer<V1alpha1View, V1alpha1ViewList> {

  private final View view;
  private final boolean materialized;

  K8sViewDeployer(View view, boolean materialized, K8sContext context) {
    super(context, K8sApiEndpoints.VIEWS);
    this.view = view;
    this.materialized = materialized;
  }

  @Override
  protected V1alpha1View toK8sObject() {
    String name = K8sUtils.canonicalizeName(view.path());
    LinkedList<String> q = new LinkedList<>(view.path());
    return new V1alpha1View()
        .kind(K8sApiEndpoints.VIEWS.kind())
        .apiVersion(K8sApiEndpoints.VIEWS.apiVersion())
        .metadata(new V1ObjectMeta()
          .name(name))
        .spec(new V1alpha1ViewSpec()
            .view(q.pollLast())
            .schema(q.pollLast())
            .catalog(q.pollLast())
            .sql(view.viewSql())
            .materialized(materialized));
  }
}
