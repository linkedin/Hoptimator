package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.util.Yaml;

import com.linkedin.hoptimator.Deployer;


public abstract class K8sDeployer<T, U extends KubernetesObject, V extends KubernetesListObject>
    implements Deployer<T> {

  private K8sApi<U, V> api;

  K8sDeployer(K8sContext context, K8sApiEndpoint<U, V> endpoint) {
    this.api = new K8sApi<U, V>(context, endpoint);
  }

  @Override
  public void create(T t) throws SQLException {
    api.create(toK8sObject(t));
  }

  @Override
  public void delete(T t) throws SQLException {
    api.delete(toK8sObject(t));
  }

  @Override
  public void update(T t) throws SQLException {
    api.update(toK8sObject(t));
  }

  @Override
  public List<String> specify(T t) throws SQLException {
    return Collections.singletonList(Yaml.dump(toK8sObject(t)));
  }

  protected abstract U toK8sObject(T t) throws SQLException;
}
