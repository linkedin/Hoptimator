package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;

import com.linkedin.hoptimator.util.RemoteTable;


public abstract class K8sTable<ObjectType extends KubernetesObject, ObjectListType extends KubernetesListObject, RowType>
    extends RemoteTable<ObjectType, RowType> {

  private final K8sContext context;
  private final K8sApiEndpoint<ObjectType, ObjectListType> endpoint;

  public K8sTable(K8sContext context, K8sApiEndpoint<ObjectType, ObjectListType> endpoint, Class<RowType> v) {
    super(new K8sApi<>(context, endpoint), v);
    this.context = context;
    this.endpoint = endpoint;
  }
}
