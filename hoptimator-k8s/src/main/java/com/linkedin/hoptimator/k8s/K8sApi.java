package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.options.ListOptions;

import com.linkedin.hoptimator.util.Api;


public class K8sApi<T extends KubernetesObject, U extends KubernetesListObject> implements Api<T> {
  private final static Logger log = LoggerFactory.getLogger(K8sApi.class);

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

  public T get(String name) throws SQLException {
    final KubernetesApiResponse<T> resp;
    if (endpoint.clusterScoped()) {
      resp = context.<T, U>generic(endpoint).get(name);
    } else {
      resp = context.<T, U>generic(endpoint).get(context.namespace(), name);
    }
    checkResponse(resp);
    return resp.getObject();
  }

  public T get(String namespace, String name) throws SQLException {
    KubernetesApiResponse<T> resp = context.<T, U>generic(endpoint).get(namespace, name);
    checkResponse(resp);
    return resp.getObject();
  }

  public T get(T obj) throws SQLException {
    if (obj.getMetadata().getNamespace() == null && !endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    final KubernetesApiResponse<T> resp;
    if (endpoint.clusterScoped()) {
      resp = context.<T, U>generic(endpoint).get(obj.getMetadata().getName());
    } else {
      resp  = context.<T, U>generic(endpoint).get(obj.getMetadata().getNamespace(), obj.getMetadata().getName());
    }
    checkResponse(resp);
    return resp.getObject();
  }

  public V1OwnerReference reference(T obj) throws SQLException {
    T existing = get(obj);
    return new V1OwnerReference()
        .kind(existing.getKind())
        .name(existing.getMetadata().getName())
        .apiVersion(existing.getApiVersion())
        .uid(existing.getMetadata().getUid());
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
    if (obj.getMetadata().getNamespace() == null && !endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    context.own(obj);
    KubernetesApiResponse<T> resp = context.<T, U>generic(endpoint).create(obj);
    checkResponse(resp);
    log.info("Created K8s obj: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  @Override
  public void delete(T obj) throws SQLException {
    if (obj.getMetadata().getNamespace() == null && !endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    KubernetesApiResponse<T> resp =
        context.<T, U>generic(endpoint).delete(obj.getMetadata().getNamespace(), obj.getMetadata().getName());
    checkResponse(resp);
    log.info("Deleted K8s obj: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  @Override
  public void update(T obj) throws SQLException {
    if (obj.getMetadata().getNamespace() == null && !endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    final KubernetesApiResponse<T> existing;
    if (endpoint.clusterScoped()) {
      existing = context.<T, U>generic(endpoint).get(obj.getMetadata().getName());
    } else {
      existing = context.<T, U>generic(endpoint).get(obj.getMetadata().getNamespace(), obj.getMetadata().getName());
    }
    final KubernetesApiResponse<T> resp;
    if (existing.isSuccess()) {

      // Ensure labels are additive.
      Map<String, String> labels = new HashMap<>();
      if (existing.getObject().getMetadata().getLabels() != null) {
        labels.putAll(existing.getObject().getMetadata().getLabels());
      }
      if (obj.getMetadata().getLabels() != null) {
        labels.putAll(obj.getMetadata().getLabels());
      }
      obj.getMetadata().setLabels(labels);

      List<V1OwnerReference> owners = new LinkedList<>();
      if (existing.getObject().getMetadata().getOwnerReferences() != null) {
        owners.addAll(existing.getObject().getMetadata().getOwnerReferences());
      }
      if (obj.getMetadata().getOwnerReferences() != null) {
        owners.addAll(obj.getMetadata().getOwnerReferences());
      }
      obj.getMetadata().setOwnerReferences(owners);

      obj.getMetadata().resourceVersion(existing.getObject().getMetadata().getResourceVersion());
      context.own(obj);
      resp = context.<T, U>generic(endpoint).update(obj);
    } else {
      context.own(obj);
      resp = context.<T, U>generic(endpoint).create(obj);
    }
    checkResponse(resp);
    log.info("Updated K8s obj: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  public void updateStatus(T obj, Object status) throws SQLException {
    if (obj.getMetadata().getNamespace() == null && !endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    KubernetesApiResponse<T> resp = context.<T, U>generic(endpoint).updateStatus(obj, x -> status);
    checkResponse(resp);
    log.info("Updated K8s obj status: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  private void checkResponse(KubernetesApiResponse<?> resp) throws SQLException {
    if (!resp.isSuccess()) {
      throw new SQLException(resp.getStatus().getMessage() + ": " + context, null, resp.getHttpStatusCode());
    }
  }
}
