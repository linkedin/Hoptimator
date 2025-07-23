package com.linkedin.hoptimator.operator;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.kubernetes.client.openapi.models.V1ConfigMap;


public class ConfigAssembler {
  private final K8sContext context;
  private final List<ConfigMapRef> refs = new ArrayList<>();
  private final Map<String, String> overrides = new HashMap<>();

  public ConfigAssembler(K8sContext context) {
    this.context = context;
  }

  public void addOverride(String key, String value) {
    overrides.put(key, value);
  }

  public void addRef(String namespace, String name) {
    refs.add(new ConfigMapRef(namespace, name));
  }

  public Map<String, String> assemble() throws SQLException {
    Map<String, String> combined = new HashMap<>();
    for (ConfigMapRef ref : refs) {
        combined.putAll(ref.fetch(context));
    }
    combined.putAll(overrides);
    return combined;
  }

  public Properties assembleProperties() throws SQLException {
    Properties properties = new Properties();
    properties.putAll(assemble());
    return properties;
  }

  private static class ConfigMapRef {
    private final String namespace;
    private final String name;

    ConfigMapRef(String namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    Map<String, String> fetch(K8sContext context) throws SQLException {
      K8sApi<V1ConfigMap, V1ConfigMapList> configMapApi = new K8sApi<>(context, K8sApiEndpoints.CONFIG_MAPS);
      return configMapApi.get(namespace, name).getData();
    }
  }
}
