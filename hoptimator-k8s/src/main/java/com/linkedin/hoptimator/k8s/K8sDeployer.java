package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.Yaml;

import com.linkedin.hoptimator.Deployer;


public abstract class K8sDeployer<T extends KubernetesObject, U extends KubernetesListObject>
    implements Deployer {

  private final K8sApi<T, U> api;

  K8sDeployer(K8sContext context, K8sApiEndpoint<T, U> endpoint) {
    this.api = new K8sApi<>(context, endpoint);
  }

  @Override
  public void create() throws SQLException {
    api.create(toK8sObject());
  }

  public V1OwnerReference createAndReference() throws SQLException {
    T obj = toK8sObject();
    api.create(obj);
    return api.reference(obj);
  }

  @Override
  public void delete() throws SQLException {
    api.delete(toK8sObject());
  }

  @Override
  public void update() throws SQLException {
    api.update(toK8sObject());
  }

  public V1OwnerReference updateAndReference() throws SQLException {
    T obj = toK8sObject();
    api.update(obj);
    return api.reference(obj);
  }

  @Override
  public List<String> specify() throws SQLException {
    return Collections.singletonList(Yaml.dump(toK8sObject()));
  }

  protected abstract T toK8sObject() throws SQLException;
}
