package com.linkedin.hoptimator.k8s;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.ConnectorProvider;
import com.linkedin.hoptimator.util.Source;


public class K8sConnectorProvider implements ConnectorProvider {

  @SuppressWarnings("unchecked")
  @Override
  public <T> Collection<Connector<T>> connectors(Class<T> clazz) {
    List<Connector<T>> list = new ArrayList<>();
    if (Source.class.isAssignableFrom(clazz)) {
      list.add((Connector<T>) new K8sConnector(K8sContext.currentContext()));
    }
    return list;
  }
}
