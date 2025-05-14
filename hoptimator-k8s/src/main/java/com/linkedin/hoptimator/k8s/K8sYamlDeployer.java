package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;

import com.linkedin.hoptimator.Deployer;


public abstract class K8sYamlDeployer implements Deployer {

  private final K8sYamlApi api;

  public K8sYamlDeployer(K8sContext context) {
    this.api = new K8sYamlApi(context);
  }

  @Override
  public void create() throws SQLException {
    for (String spec : specify()) {
      api.create(spec);
    }
  }

  @Override
  public void delete() throws SQLException {
    for (String spec : specify()) {
      api.delete(spec);
    }
  }

  @Override
  public void update() throws SQLException {
    for (String spec : specify()) {
      api.update(spec);
    }
  }
}
