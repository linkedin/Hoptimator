package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.ConnectorProvider;
import com.linkedin.hoptimator.DeploymentContext;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;


public final class ConnectionService {

  private ConnectionService() {
  }

  public static <T> Map<String, String> configure(T obj, DeploymentContext context)
        throws SQLException {
    Map<String, String> configs = new LinkedHashMap<>();
    for (Connector connector : connectors(obj, context)) {
      configs.putAll(connector.configure());
    }
    return configs;
  }

  public static Collection<ConnectorProvider> providers() {
    ServiceLoader<ConnectorProvider> loader = ServiceLoader.load(ConnectorProvider.class);
    List<ConnectorProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(providers::add);
    return providers;
  }

  public static <T> Collection<Connector> connectors(T obj, DeploymentContext context) throws SQLException {
    List<Connector> connectors = new ArrayList<>();
    for (ConnectorProvider provider : providers()) {
      connectors.addAll(provider.connectors(obj, context));
    }
    return connectors;
  }
}
