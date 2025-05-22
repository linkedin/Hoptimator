package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.SnapshotService;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import java.sql.SQLException;

import com.linkedin.hoptimator.Deployer;


public abstract class K8sYamlDeployer implements Deployer {

  private final K8sYamlApi api;
  private final K8sContext context;

  public K8sYamlDeployer(K8sContext context) {
    this.api = new K8sYamlApi(context);
    this.context = context;
  }

  @Override
  public void create() throws SQLException {
    for (String spec : specify()) {
      DynamicKubernetesObject obj = api.objFromYaml(spec);
      SnapshotService.store(obj, context.connectionProperties());
      api.create(obj);
    }
  }

  @Override
  public void delete() throws SQLException {
    for (String spec : specify()) {
      DynamicKubernetesObject obj = api.objFromYaml(spec);
      SnapshotService.store(obj, context.connectionProperties());
      api.delete(obj);
    }
  }

  @Override
  public void update() throws SQLException {
    for (String spec : specify()) {
      DynamicKubernetesObject obj = api.objFromYaml(spec);
      SnapshotService.store(obj, context.connectionProperties());
      api.update(obj);
    }
  }
}
