package com.linkedin.hoptimator.k8s;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.calcite.schema.impl.ViewTable;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.DeployerProvider;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.MaterializedView;
import com.linkedin.hoptimator.Source;

public class K8sDeployerProvider implements DeployerProvider {

  @SuppressWarnings("unchecked")
  @Override
  public <T> Collection<Deployer<T>> deployers(Class<T> clazz, Properties connectionProperties) {
    K8sContext context = new K8sContext(connectionProperties);
    List<Deployer<T>> list = new ArrayList<>();
    if (ViewTable.class.isAssignableFrom(clazz)) {
      list.add((Deployer<T>) new K8sViewDeployer(context));
    }
    if (MaterializedView.class.isAssignableFrom(clazz)) {
      list.add((Deployer<T>) new K8sMaterializedViewDeployer(context));
      list.add((Deployer<T>) new K8sPipelineDeployer(context));
    }
    if (Source.class.isAssignableFrom(clazz)) {
      list.add((Deployer<T>) new K8sSourceDeployer(context));
    }
    if (Job.class.isAssignableFrom(clazz)) {
      list.add((Deployer<T>) new K8sJobDeployer(context));
    }
    return list;
  }
}
