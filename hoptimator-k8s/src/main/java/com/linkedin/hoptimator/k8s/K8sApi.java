package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.Api;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.options.DeleteOptions;
import io.kubernetes.client.util.generic.options.ListOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;


public class K8sApi<T extends KubernetesObject, U extends KubernetesListObject> implements Api<T> {
  private static final Logger log = LoggerFactory.getLogger(K8sApi.class);

  private final K8sContext context;
  private final K8sApiEndpoint<T, U> endpoint;

  public K8sApi(K8sContext context, K8sApiEndpoint<T, U> endpoint) {
    this.context = context;
    this.endpoint = endpoint;
  }

  public K8sApiEndpoint<T, U> endpoint() {
    return endpoint;
  }

  /**
   * Executes a Kubernetes client call, normalizing connectivity failures to a typed transient error.
   *
   * <p>The Kubernetes client reports a genuine connectivity failure (connection refused, unknown
   * host, socket timeout) as an unchecked {@link IllegalStateException} wrapping an
   * {@link IOException}, thrown from the terminal {@code generic().list/get/create/delete/update}
   * call — HTTP-status errors (404, 409, ...) instead come back in the response and are classified by
   * {@link K8sUtils#checkResponse}. Left unchecked, the connectivity failure would escape the
   * {@link SQLException} contract as an unclassified error, and — worse — a swallowed read could
   * report success while nothing was actually resolved or deployed. So this wraps every backend call,
   * giving all SPIs (config resolution and deployer execution alike) a uniform classification: an
   * IOException-caused failure is a {@link SQLTransientException} (a retryable connectivity blip);
   * any other {@link IllegalStateException} is surfaced as a {@link SQLNonTransientException} rather
   * than being masked as retryable.
   */
  private <R> R call(String action, Supplier<R> request) throws SQLException {
    try {
      return request.get();
    } catch (IllegalStateException e) {
      if (e.getCause() instanceof IOException) {
        throw new SQLTransientException("Could not reach Kubernetes to " + action + ": " + e.getMessage(), e);
      }
      throw new SQLNonTransientException("Kubernetes call failed to " + action + ": " + e.getMessage(), e);
    }
  }

  @Override
  public Collection<T> list() throws SQLException {
    return select(null);
  }

  public T get(String name) throws SQLException {
    final KubernetesApiResponse<T> resp;
    if (endpoint.clusterScoped()) {
      resp = call("get " + endpoint().kind() + " " + name, () -> context.generic(endpoint).get(name));
    } else {
      resp = call("get " + endpoint().kind() + " " + name,
          () -> context.generic(endpoint).get(context.namespace(), name));
    }
    K8sUtils.checkResponse("Error getting " + endpoint().kind() + " " + name, resp);
    return resp.getObject();
  }

  public T get(String namespace, String name) throws SQLException {
    KubernetesApiResponse<T> resp = call("get " + endpoint().kind() + " " + name,
        () -> context.generic(endpoint).get(namespace, name));
    K8sUtils.checkResponse("Error getting " + endpoint().kind() + " " + name, resp);
    return resp.getObject();
  }

  public T getIfExists(String namespace, String name) throws SQLException {
    KubernetesApiResponse<T> resp = call("get " + endpoint().kind() + " " + name,
        () -> context.generic(endpoint).get(namespace, name));
    if (resp.getHttpStatusCode() == 404) {
      return null;
    }
    K8sUtils.checkResponse("Error getting " + endpoint().kind() + " " + name, resp);
    return resp.getObject();
  }

  public T getIfExists(String name) throws SQLException {
    final KubernetesApiResponse<T> resp;
    if (endpoint.clusterScoped()) {
      resp = call("get " + endpoint().kind() + " " + name, () -> context.generic(endpoint).get(name));
    } else {
      resp = call("get " + endpoint().kind() + " " + name,
          () -> context.generic(endpoint).get(context.namespace(), name));
    }
    if (resp.getHttpStatusCode() == 404) {
      return null;
    }
    K8sUtils.checkResponse("Error getting " + endpoint().kind() + " " + name, resp);
    return resp.getObject();
  }

  public T get(T obj) throws SQLException {
    if (obj.getMetadata().getNamespace() == null && !endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    final KubernetesApiResponse<T> resp;
    String name = obj.getMetadata().getName();
    if (endpoint.clusterScoped()) {
      resp = call("get " + name, () -> context.generic(endpoint).get(name));
    } else {
      resp = call("get " + name,
          () -> context.generic(endpoint).get(obj.getMetadata().getNamespace(), name));
    }
    K8sUtils.checkResponse("Error getting " + obj.getMetadata().getName(), resp);
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
    GenericKubernetesApi<T, U> generic = context.generic(endpoint);
    ListOptions options = new ListOptions();
    options.setLabelSelector(labelSelector);
    final KubernetesApiResponse<U> resp;
    if (endpoint.clusterScoped()) {
      resp = call("list " + endpoint().kind(), () -> generic.list(options));
    } else {
      resp = call("list " + endpoint().kind(), () -> generic.list(context.namespace(), options));
    }
    if (resp.getHttpStatusCode() == 404) {
      return Collections.emptyList();
    }
    K8sUtils.checkResponse("Error selecting " + labelSelector, resp);
    return (Collection<T>) resp.getObject().getItems();
  }

  @Override
  public void create(T obj) throws SQLException {
    if (obj.getMetadata().getNamespace() == null && !endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    context.own(obj);
    KubernetesApiResponse<T> resp =
        call("create " + obj.getMetadata().getName(), () -> context.generic(endpoint).create(obj));
    K8sUtils.checkResponse("Error creating " + obj.getMetadata().getName(), resp);
    log.info("Created K8s obj: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  @Override
  public void delete(T obj) throws SQLException {
    if (obj.getMetadata().getNamespace() == null && !endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    delete(obj.getMetadata().getNamespace(), obj.getMetadata().getName());
  }

  public void delete(String namespace, String name) throws SQLException {
    DeleteOptions options = new DeleteOptions();
    options.setPropagationPolicy("Background");   // ensure deletes cascade
    KubernetesApiResponse<T> resp =
        call("delete " + name, () -> context.generic(endpoint).delete(namespace, name, options));
    K8sUtils.checkResponse("Error deleting " + name, resp);
    log.info("Deleted K8s obj: {}:{}", endpoint.kind(), name);
  }

  public void delete(String name) throws SQLException {
    delete(context.namespace(), name);
  }

  @Override
  public void update(T obj) throws SQLException {
    if (obj.getMetadata().getNamespace() == null && !endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    final KubernetesApiResponse<T> existing;
    if (endpoint.clusterScoped()) {
      existing = call("get " + obj.getMetadata().getName(),
          () -> context.generic(endpoint).get(obj.getMetadata().getName()));
    } else {
      existing = call("get " + obj.getMetadata().getName(),
          () -> context.generic(endpoint).get(obj.getMetadata().getNamespace(), obj.getMetadata().getName()));
    }
    final KubernetesApiResponse<T> resp;
    if (existing.isSuccess()) {

      // Ensure labels, annotations, and owners are additive.
      Map<String, String> labels = new HashMap<>();
      if (existing.getObject().getMetadata().getLabels() != null) {
        labels.putAll(existing.getObject().getMetadata().getLabels());
      }
      if (obj.getMetadata().getLabels() != null) {
        labels.putAll(obj.getMetadata().getLabels());
      }
      obj.getMetadata().setLabels(labels);

      Map<String, String> annotations = new HashMap<>();
      if (existing.getObject().getMetadata().getAnnotations() != null) {
        annotations.putAll(existing.getObject().getMetadata().getAnnotations());
      }
      if (obj.getMetadata().getAnnotations() != null) {
        annotations.putAll(obj.getMetadata().getAnnotations());
      }
      obj.getMetadata().setAnnotations(annotations);

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
      resp = call("update " + obj.getMetadata().getName(), () -> context.generic(endpoint).update(obj));
    } else {
      context.own(obj);
      resp = call("create " + obj.getMetadata().getName(), () -> context.generic(endpoint).create(obj));
    }
    K8sUtils.checkResponse("Error updating " + obj.getMetadata().getName(), resp);
    log.info("Updated K8s obj: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  public void updateStatus(T obj, Object status) throws SQLException {
    if (obj.getMetadata().getNamespace() == null && !endpoint.clusterScoped()) {
      obj.getMetadata().namespace(context.namespace());
    }
    KubernetesApiResponse<T> resp =
        call("update status of " + obj.getMetadata().getName(),
            () -> context.generic(endpoint).updateStatus(obj, x -> status));
    K8sUtils.checkResponse(() -> "Error updating status of " + Yaml.dump(obj), resp);
    log.info("Updated K8s obj status: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }
}
