package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.SnapshotService;
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
  private final K8sContext context;

  K8sDeployer(K8sContext context, K8sApiEndpoint<T, U> endpoint) {
    this.api = new K8sApi<>(context, endpoint);
    this.context = context;
  }

  @Override
  public void create() throws SQLException {
    create(toK8sObject());
  }

  public V1OwnerReference createAndReference() throws SQLException {
    T obj = toK8sObject();
    create(obj);
    return api.reference(obj);
  }

  private void create(T obj) throws SQLException {
    SnapshotService.store(obj, context.connectionProperties());
    api.create(obj);
  }

  @Override
  public void delete() throws SQLException {
    T obj = toK8sObject();
    SnapshotService.store(obj, context.connectionProperties());
    api.delete(obj);
  }

  @Override
  public void update() throws SQLException {
    update(toK8sObject());
  }

  public V1OwnerReference updateAndReference() throws SQLException {
    T obj = toK8sObject();
    update(obj);
    return api.reference(obj);
  }

  private void update(T obj) throws SQLException {
    SnapshotService.store(obj, context.connectionProperties());
    api.update(obj);
  }

  @Override
  public List<String> specify() throws SQLException {
    return Collections.singletonList(Yaml.dump(toK8sObject()));
  }

  protected abstract T toK8sObject() throws SQLException;
}
