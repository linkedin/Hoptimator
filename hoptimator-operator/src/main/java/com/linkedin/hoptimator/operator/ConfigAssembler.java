package com.linkedin.hoptimator.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.kubernetes.client.openapi.models.V1ConfigMap;


public class ConfigAssembler {
  private final Operator operator;
  private final List<ConfigMapRef> refs = new ArrayList<>();
  private final Map<String, String> overrides = new HashMap<>();

  public ConfigAssembler(Operator operator) {
    this.operator = operator;
  }

  public void addOverride(String key, String value) {
    overrides.put(key, value);
  }

  public void addRef(String namespace, String name) {
    refs.add(new ConfigMapRef(namespace, name));
  }

  public Map<String, String> assemble() {
    Map<String, String> combined = new HashMap<>();
    refs.forEach(x -> combined.putAll(x.fetch(operator)));
    overrides.forEach((k, v) -> combined.put(k, v));
    return combined;
  }

  public Properties assembleProperties() {
    Properties properties = new Properties();
    assemble().forEach((k, v) -> properties.put(k, v));
    return properties;
  }

  private static class ConfigMapRef {
    private final String namespace;
    private final String name;

    ConfigMapRef(String namespace, String name) {
      this.namespace = namespace;
      this.name = name;
    }

    Map<String, String> fetch(Operator operator) {
      return operator.<V1ConfigMap>fetch("configmap", namespace, name).getData();
    }
  }
}
