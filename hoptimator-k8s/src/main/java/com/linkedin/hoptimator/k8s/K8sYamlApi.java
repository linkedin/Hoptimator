package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.Api;

import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;

import java.util.Collection;
import java.util.Locale;
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
        guessPlural(obj)).create(obj);
    checkResponse(resp);
  }

  @Override
  public void delete(String yaml) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
    KubernetesApiResponse<DynamicKubernetesObject> resp = context.dynamic(obj.getApiVersion(),
        guessPlural(obj)).delete(obj.getMetadata().getName());
    checkResponse(resp);
  }

  @Override
  public void update(String yaml) throws SQLException {
    DynamicKubernetesObject obj = objFromYaml(yaml);
    KubernetesApiResponse<DynamicKubernetesObject> resp = context.dynamic(obj.getApiVersion(),
        guessPlural(obj)).update(obj);
    checkResponse(resp);
  }

  private String guessPlural(DynamicKubernetesObject obj) {
    String lower = obj.getKind().toLowerCase(Locale.ROOT);
    if (lower.endsWith("y")) {
      return lower.substring(0, lower.length() - 1) + "ies";
    } else {
      return lower + "s";
    }
  }

  private DynamicKubernetesObject objFromYaml(String yaml) {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    obj.getMetadata().namespace(context.namespace());
    return obj;
  }
 
  private void checkResponse(KubernetesApiResponse<?> resp) throws SQLException {
    if (!resp.isSuccess()) {
      throw new SQLException(resp.getStatus().getMessage());
    }
  }
}
