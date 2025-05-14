package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;

import com.linkedin.hoptimator.util.RemoteTable;


public abstract class K8sTable<OBJECT_TYPE extends KubernetesObject, OBJECT_LIST_TYPE extends KubernetesListObject, ROW_TYPE>
    extends RemoteTable<OBJECT_TYPE, ROW_TYPE> {

  private final K8sContext context;
  private final K8sApiEndpoint<OBJECT_TYPE, OBJECT_LIST_TYPE> endpoint;

  public K8sTable(K8sContext context, K8sApiEndpoint<OBJECT_TYPE, OBJECT_LIST_TYPE> endpoint, Class<ROW_TYPE> v) {
    super(new K8sApi<>(context, endpoint), v);
    this.context = context;
    this.endpoint = endpoint;
  }
}
