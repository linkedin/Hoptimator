package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.Source;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.util.MaterializedView;

import org.apache.calcite.schema.impl.ViewTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class K8sDeployerProvider implements DeployerProvider {

  @SuppressWarnings("unchecked")
  @Override
  public <T> Collection<Deployer<T>> deployers(Class<T> clazz) {
    List<Deployer<T>> list = new ArrayList<>();
    if (ViewTable.class.isAssignableFrom(clazz)) {
      list.add((Deployer<T>) new K8sViewDeployer(K8sContext.currentContext()));
    }
    if (MaterializedView.class.isAssignableFrom(clazz)) {
      list.add((Deployer<T>) new K8sMaterializedViewDeployer(K8sContext.currentContext()));
      list.add((Deployer<T>) new K8sPipelineDeployer(K8sContext.currentContext()));
    }
    if (Source.class.isAssignableFrom(clazz)) {
      list.add((Deployer<T>) new K8sSourceDeployer(K8sContext.currentContext()));
    }
    return list;
  }
}
