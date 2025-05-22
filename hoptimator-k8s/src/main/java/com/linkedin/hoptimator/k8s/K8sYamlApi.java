package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;

import com.linkedin.hoptimator.util.Api;


public class K8sYamlApi implements Api<String> {
  private static final Logger log = LoggerFactory.getLogger(K8sYamlApi.class);

  private final K8sContext context;

  public K8sYamlApi(K8sContext context) {
    this.context = context;
  }

  @Override
  public Collection<String> list() throws SQLException {
    throw new UnsupportedOperationException("Cannot list a dynamic YAML API.");
  }

  // Returns the K8s Object or null if it does not exist.
  public DynamicKubernetesObject getIfExists(String yaml) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
    return getIfExists(obj);
  }

  public DynamicKubernetesObject getIfExists(DynamicKubernetesObject obj) throws SQLException {
    KubernetesApiResponse<DynamicKubernetesObject> resp =
        context.dynamic(obj.getApiVersion(), K8sUtils.guessPlural(obj)).get(obj.getMetadata().getNamespace(),
            obj.getMetadata().getName());
    if (resp.getHttpStatusCode() == 404) {
      return null;
    }
    K8sUtils.checkResponse(String.format("Error getting K8s obj: %s:%s", obj.getKind(), obj.getMetadata().getName()), resp);
    return resp.getObject();
  }

  @Override
  public void create(String yaml) throws SQLException {
    createWithAnnotationsAndLabels(yaml, null, null);
  }

  public void create(DynamicKubernetesObject obj) throws SQLException {
    createWithAnnotationsAndLabels(obj, null, null);
  }

  public void createWithAnnotationsAndLabels(String yaml, Map<String, String> annotations,
      Map<String, String> labels) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
    createWithAnnotationsAndLabels(obj, annotations, labels);
  }

  public void createWithAnnotationsAndLabels(DynamicKubernetesObject obj, Map<String, String> annotations,
      Map<String, String> labels) throws SQLException {
    context.own(obj);
    obj.setMetadata(obj.getMetadata().annotations(annotations).labels(labels));
    KubernetesApiResponse<DynamicKubernetesObject> resp =
        context.dynamic(obj.getApiVersion(), K8sUtils.guessPlural(obj)).create(obj);
    K8sUtils.checkResponse(String.format("Error creating K8s obj: %s:%s", obj.getKind(), obj.getMetadata().getName()), resp);
    log.info("Created K8s obj: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  @Override
  public void delete(String yaml) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
    delete(obj);
  }

  public void delete(DynamicKubernetesObject obj) throws SQLException {
    delete(obj.getApiVersion(), obj.getKind(), obj.getMetadata().getNamespace(), obj.getMetadata().getName());
  }

  public void delete(String apiVersion, String kind, String namespace, String name) throws SQLException {
    KubernetesApiResponse<DynamicKubernetesObject> resp =
        context.dynamic(apiVersion, K8sUtils.guessPlural(kind)).delete(namespace, name);
    K8sUtils.checkResponse(String.format("Error getting K8s obj: %s:%s", kind, name), resp);
    log.info("Deleted K8s obj: {}:{}", kind, name);
  }

  @Override
  public void update(String yaml) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
    update(obj);
  }

  public void update(DynamicKubernetesObject obj) throws SQLException {
    KubernetesApiResponse<DynamicKubernetesObject> existing =
        context.dynamic(obj.getApiVersion(), K8sUtils.guessPlural(obj))
            .get(obj.getMetadata().getNamespace(), obj.getMetadata().getName());
    final KubernetesApiResponse<DynamicKubernetesObject> resp;
    if (existing.isSuccess()) {

      // Ensure labels are additive. Existing values are kept.
      Map<String, String> labels = new HashMap<>();
      if (obj.getMetadata().getLabels() != null) {
        labels.putAll(obj.getMetadata().getLabels());
      }
      if (existing.getObject().getMetadata().getLabels() != null) {
        labels.putAll(existing.getObject().getMetadata().getLabels());
      }
      existing.getObject().getMetadata().setLabels(labels);

      obj.setMetadata(existing.getObject().getMetadata());
      resp = context.dynamic(obj.getApiVersion(), K8sUtils.guessPlural(obj)).update(obj);
    } else {
      context.own(obj);
      resp = context.dynamic(obj.getApiVersion(), K8sUtils.guessPlural(obj)).create(obj);
    }
    K8sUtils.checkResponse(String.format("Error updating K8s obj: %s:%s", obj.getKind(), obj.getMetadata().getName()), resp);
    log.info("Updated K8s obj: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  public DynamicKubernetesObject objFromYaml(String yaml) {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    return setNamespaceFromContext(obj);
  }

  public DynamicKubernetesObject setNamespaceFromContext(DynamicKubernetesObject obj) {
    if (obj.getMetadata().getNamespace() == null) {
      obj.setMetadata(obj.getMetadata().namespace(context.namespace()));
    }
    return obj;
  }
}
