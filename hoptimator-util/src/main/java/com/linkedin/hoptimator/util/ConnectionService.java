package com.linkedin.hoptimator.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.ConnectorProvider;


public final class ConnectionService {

  private ConnectionService() {
  }

  public static <T> Map<String, String> configure(T object, Class<T> clazz) throws SQLException {
    Map<String, String> configs = new HashMap<>();
    for (Connector<T> connector : connectors(clazz)) {
      configs.putAll(connector.configure(object));
    }
    return configs;
  }

  public static Collection<ConnectorProvider> providers() {
    ServiceLoader<ConnectorProvider> loader = ServiceLoader.load(ConnectorProvider.class);
    List<ConnectorProvider> providers = new ArrayList<>();
    loader.iterator().forEachRemaining(providers::add);
    return providers;
  }

  public static <T> Collection<Connector<T>> connectors(Class<T> clazz) {
    return providers().stream().flatMap(x -> x.connectors(clazz).stream()).collect(Collectors.toList());
  }
}
