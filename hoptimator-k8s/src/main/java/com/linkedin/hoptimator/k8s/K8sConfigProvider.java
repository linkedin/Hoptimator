package com.linkedin.hoptimator.k8s;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;

import com.linkedin.hoptimator.ConfigProvider;


public class K8sConfigProvider implements ConfigProvider {

  public static final String HOPTIMATOR_CONFIG_MAP = "hoptimator-configmap";

  public Map<String, String> loadConfig() throws Exception {
    return loadConfig(HOPTIMATOR_CONFIG_MAP);
  }

  // Load top-level config map properties
  public Map<String, String> loadConfig(String configMapName) throws Exception {
    K8sApi<V1ConfigMap, V1ConfigMapList> configMapApi = new K8sApi<>(K8sContext.currentContext(), K8sApiEndpoints.CONFIG_MAP_TEMPLATES);
    return configMapApi.get(configMapName).getData();
  }

  // Load config map properties under a file-like property key
  // Ex:
  //  log.properties: |
  //    level=INFO
  public Properties loadConfigMapPropertiesFile(String configMapName, String filePropertyName) throws Exception {
    Map<String, String> configMap = loadConfig(configMapName);
    if (configMap == null || !configMap.containsKey(filePropertyName)) {
      throw new SQLException("Config map does not contain " + filePropertyName);
    }
    Properties p = new Properties();
    p.load(new StringReader(configMap.get(filePropertyName)));
    return p;
  }
}
