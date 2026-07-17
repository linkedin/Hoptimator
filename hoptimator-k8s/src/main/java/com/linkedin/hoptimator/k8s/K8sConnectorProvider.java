package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.ConnectorProvider;
import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class K8sConnectorProvider implements ConnectorProvider {

  @Override
  public <T> Collection<Connector> connectors(T obj, DeploymentContext deploymentContext) {
    K8sContext context = K8sContext.create(deploymentContext);
    List<Connector> list = new ArrayList<>();
    if (obj instanceof Source) {
      list.add(new K8sConnector((Source) obj, context));
    }
    return list;
  }
}
