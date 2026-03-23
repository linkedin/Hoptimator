package com.linkedin.hoptimator.k8s;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;

import com.linkedin.hoptimator.ConfigProvider;


public class K8sConfigProvider implements ConfigProvider {

  public static final String HOPTIMATOR_CONFIG_MAP = "hoptimator-configmap";

  public Properties loadConfig(Connection connection) throws SQLException {
    Map<String, String> topLevelConfigs = loadTopLevelConfig(HOPTIMATOR_CONFIG_MAP, connection);
    Properties p = new Properties();
    p.putAll(topLevelConfigs);
    return p;
  }

  // Package-private factory method — override in tests to inject a mock K8sApi
  K8sApi<V1ConfigMap, V1ConfigMapList> createConfigMapApi(K8sContext context) {
    return new K8sApi<>(context, K8sApiEndpoints.CONFIG_MAPS);
  }

  private Map<String, String> loadTopLevelConfig(String configMapName, Connection connection) throws SQLException {
    K8sContext context = K8sContext.create(connection);
    K8sApi<V1ConfigMap, V1ConfigMapList> configMapApi = createConfigMapApi(context);
    String namespace = context.namespace();
    if (namespace == null || namespace.isEmpty()) {
      return configMapApi.get(configMapName).getData();
    }
    return configMapApi.get(namespace, configMapName).getData();
  }
}
