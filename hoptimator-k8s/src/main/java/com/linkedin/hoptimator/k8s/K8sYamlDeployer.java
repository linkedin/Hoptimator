package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;

import com.linkedin.hoptimator.Deployer;


public abstract class K8sYamlDeployer<T> implements Deployer<T> {

  private K8sYamlApi api;

  public K8sYamlDeployer(K8sContext context) {
    this.api = new K8sYamlApi(context);
  }

  @Override
  public void create(T t) throws SQLException {
    for (String spec : specify(t)) {
      api.create(spec);
    }
  }

  @Override
  public void delete(T t) throws SQLException {
    for (String spec : specify(t)) {
      api.delete(spec);
    }
  }

  @Override
  public void update(T t) throws SQLException {
    for (String spec : specify(t)) {
      api.update(spec);
    }
  }
}
