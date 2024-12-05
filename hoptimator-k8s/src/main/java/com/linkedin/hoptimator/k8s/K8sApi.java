package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.Api;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.options.ListOptions;

import java.util.Collection;
import java.util.Collections;
import java.sql.SQLException;

public class K8sApi<T extends KubernetesObject, U extends KubernetesListObject> implements Api<T> {

  private final K8sContext context;
  private final K8sApiEndpoint<T, U> endpoint;

  public K8sApi(K8sContext context, K8sApiEndpoint<T, U> endpoint) {
    this.context = context;
    this.endpoint = endpoint;
  }

  public K8sApiEndpoint<T, U> endpoint() {
    return endpoint;
  }

  @Override
  public Collection<T> list() throws SQLException {
    return select(null);
  }

  @SuppressWarnings("unchecked")
  public Collection<T> select(String labelSelector) throws SQLException {
    GenericKubernetesApi<T, U> generic = context.<T, U>generic(endpoint);
    ListOptions options = new ListOptions();
    options.setLabelSelector(labelSelector);
    final KubernetesApiResponse<U> resp;
    if (endpoint.clusterScoped()) {
      resp = generic.list(options);
    } else {
      resp = generic.list(context.namespace(), options);
    }
    if (resp.getHttpStatusCode() == 404) {
      return Collections.emptyList();
    }
    checkResponse(resp);
    return (Collection<T>) resp.getObject().getItems();
  }

  @Override
  public void create(T obj) throws SQLException {
    if (!endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    KubernetesApiResponse<T> resp = context.<T, U>generic(endpoint).create(obj);
    checkResponse(resp);
  }

  @Override
  public void delete(T obj) throws SQLException {
    if (!endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    KubernetesApiResponse<T> resp = context.<T, U>generic(endpoint).delete(
        obj.getMetadata().getNamespace(), obj.getMetadata().getName());
    checkResponse(resp);
  }

  @Override
  public void update(T obj) throws SQLException {
    if (!endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    KubernetesApiResponse<T> resp = context.<T, U>generic(endpoint).update(obj);
    checkResponse(resp);
  }

  private void checkResponse(KubernetesApiResponse<?> resp) throws SQLException {
    if (!resp.isSuccess()) {
      throw new SQLException(resp.getStatus().getMessage());
    }
  }
}
