package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.ConnectorProvider;
import com.linkedin.hoptimator.Source;


public class K8sConnectorProvider implements ConnectorProvider {

  @Override
  public <T> Collection<Connector> connectors(T obj, Connection connection) {
    K8sContext context = K8sContext.create(connection);
    List<Connector> list = new ArrayList<>();
    if (obj instanceof Source) {
      list.add(new K8sConnector((Source) obj, context));
    }
    return list;
  }
}
