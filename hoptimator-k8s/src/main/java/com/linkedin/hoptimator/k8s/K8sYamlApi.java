package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.Api;

import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;
import io.kubernetes.client.openapi.ApiException;

import java.util.Collection;
import java.sql.SQLException;

public class K8sYamlApi implements Api<String> {

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
    KubernetesApiResponse<DynamicKubernetesObject> resp = context.dynamic(obj.getApiVersion(),
        K8sUtils.guessPlural(obj)).create(obj);
    checkResponse(yaml, resp);
  }

  @Override
  public void delete(String yaml) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
    KubernetesApiResponse<DynamicKubernetesObject> resp = context.dynamic(obj.getApiVersion(),
        K8sUtils.guessPlural(obj)).delete(obj.getMetadata().getNamespace(),
        obj.getMetadata().getName());
    checkResponse(yaml, resp);
  }

  @Override
  public void update(String yaml) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
    KubernetesApiResponse<DynamicKubernetesObject> existing = context.dynamic(obj.getApiVersion(),
        K8sUtils.guessPlural(obj)).get(obj.getMetadata().getNamespace(),
        obj.getMetadata().getName());
    final KubernetesApiResponse<DynamicKubernetesObject> resp;
    if (existing.isSuccess()) {
      obj.setMetadata(existing.getObject().getMetadata());
      if (obj.getMetadata().getResourceVersion() == null) {
        throw new IllegalArgumentException("Wat.");
      }
      resp = context.dynamic(obj.getApiVersion(),
          K8sUtils.guessPlural(obj)).update(obj);
    } else {
      resp = context.dynamic(obj.getApiVersion(),
          K8sUtils.guessPlural(obj)).create(obj);
    }
    checkResponse(yaml, resp);
  }
 
  private DynamicKubernetesObject objFromYaml(String yaml) {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    if (obj.getMetadata().getNamespace() == null) {
      obj.getMetadata().namespace(context.namespace());
    }
    return obj;
  }
 
  private void checkResponse(String yaml, KubernetesApiResponse<?> resp) throws SQLException {
    try {
      resp.throwsApiException();
    } catch (ApiException e) {
      throw new SQLException("Error applying Kubernetes YAML:\n" + yaml, e);
    }
  }
}
