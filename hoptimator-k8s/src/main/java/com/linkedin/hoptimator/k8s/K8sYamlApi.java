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

  @Override
  public void create(String yaml) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
    context.own(obj);
    KubernetesApiResponse<DynamicKubernetesObject> resp =
        context.dynamic(obj.getApiVersion(), K8sUtils.guessPlural(obj)).create(obj);
    K8sUtils.checkResponse("Error creating YAML:\n" + yaml, resp);
    log.info("Created K8s obj: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  @Override
  public void delete(String yaml) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
    KubernetesApiResponse<DynamicKubernetesObject> resp =
        context.dynamic(obj.getApiVersion(), K8sUtils.guessPlural(obj))
            .delete(obj.getMetadata().getNamespace(), obj.getMetadata().getName());
    K8sUtils.checkResponse("Error deleting YAML:\n" + yaml, resp);
    log.info("Deleted K8s obj: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  @Override
  public void update(String yaml) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
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
    K8sUtils.checkResponse("Error updating YAML:\n" + yaml, resp);
    log.info("Updated K8s obj: {}:{}", obj.getKind(), obj.getMetadata().getName());
  }

  private DynamicKubernetesObject objFromYaml(String yaml) {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    if (obj.getMetadata().getNamespace() == null) {
      obj.setMetadata(obj.getMetadata().namespace(context.namespace()));
    }
    return obj;
  }
}
