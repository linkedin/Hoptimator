package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;

import com.linkedin.hoptimator.ConfigProvider;


public class K8sConfigProvider implements ConfigProvider {

  public static final String HOPTIMATOR_CONFIG_MAP = "hoptimator-configmap";

  public Properties loadConfig() throws SQLException {
    Map<String, String> topLevelConfigs = loadTopLevelConfig(HOPTIMATOR_CONFIG_MAP);
    Properties p = new Properties();
    p.putAll(topLevelConfigs);
    return p;
  }

  // Load top-level config map properties
  private Map<String, String> loadTopLevelConfig(String configMapName) throws SQLException {
    K8sApi<V1ConfigMap, V1ConfigMapList> configMapApi = new K8sApi<>(K8sContext.currentContext(), K8sApiEndpoints.CONFIG_MAPS);
    return configMapApi.get(configMapName).getData();
  }
}
