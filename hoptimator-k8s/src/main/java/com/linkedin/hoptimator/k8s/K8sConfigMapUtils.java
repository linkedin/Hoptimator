package com.linkedin.hoptimator.k8s;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;


public final class K8sConfigMapUtils {

  private K8sConfigMapUtils() {
  }

  public static final String HOPTIMATOR_CONFIG_MAP = "hoptimator-configmap";

  // Load top-level config map properties
  public static Map<String, String> loadConfigMap(String configMapName) throws SQLException {
    K8sApi<V1ConfigMap, V1ConfigMapList> configMapApi = new K8sApi<>(K8sContext.currentContext(), K8sApiEndpoints.CONFIG_MAP_TEMPLATES);
    return configMapApi.get(configMapName).getData();
  }

  // Load config map properties under a file-like property key
  // Ex:
  //  log.properties: |
  //    level=INFO
  public static Properties loadConfigMapPropertiesFile(String configMapName, String filePropertyName)
      throws SQLException, IOException {
    Map<String, String> configMap = loadConfigMap(configMapName);
    if (configMap == null || !configMap.containsKey(filePropertyName)) {
      throw new SQLException("Config map does not contain " + filePropertyName);
    }
    Properties p = new Properties();
    p.load(new StringReader(configMap.get(filePropertyName)));
    return p;
  }
}
