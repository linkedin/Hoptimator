package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;

import com.linkedin.hoptimator.util.RemoteTable;


public abstract class K8sTable<T extends KubernetesObject, U extends KubernetesListObject, V>
    extends RemoteTable<T, V> {

  private final K8sContext context;
  private final K8sApiEndpoint endpoint;

  public K8sTable(K8sContext context, K8sApiEndpoint<T, U> endpoint, Class<V> v) {
    super(new K8sApi<T, U>(context, endpoint), v);
    this.context = context;
    this.endpoint = endpoint;
  }
}
