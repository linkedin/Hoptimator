package com.linkedin.hoptimator.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.ConnectorProvider;


public final class ConnectionService {

  private ConnectionService() {
  }

  public static <T> Map<String, String> configure(T obj, Connection connection)
        throws SQLException {
    Map<String, String> configs = new LinkedHashMap<>();
    for (Connector connector : connectors(obj, connection)) {
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

  public static <T> Collection<Connector> connectors(T obj, Connection connection) {
    return providers().stream()
        .flatMap(x -> x.connectors(obj, connection).stream())
        .collect(Collectors.toList());
  }
}
