package com.linkedin.hoptimator.k8s;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.ConnectorProvider;
import com.linkedin.hoptimator.Source;


public class K8sConnectorProvider implements ConnectorProvider {

  @SuppressWarnings("unchecked")
  @Override
  public <T> Collection<Connector<T>> connectors(Class<T> clazz, Properties connectionProperties) {
    K8sContext context = new K8sContext(connectionProperties);
    List<Connector<T>> list = new ArrayList<>();
    if (Source.class.isAssignableFrom(clazz)) {
      list.add((Connector<T>) new K8sConnector(context));
    }
    return list;
  }
}
