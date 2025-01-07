package com.linkedin.hoptimator.k8s;

import java.io.StringReader;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;

import com.linkedin.hoptimator.ConfigProvider;


public class K8sConfigProvider implements ConfigProvider {

  private static final Logger log = LoggerFactory.getLogger(K8sConfigProvider.class);
  public static final String HOPTIMATOR_CONFIG_MAP = "hoptimator-configmap";

  // Loads top level configs and expands given properties under a file-like property key
  // Ex:
  //  log.properties: |
  //    level=INFO
  public Properties loadConfig(String... expansionFields) throws Exception {
    Map<String, String> topLevelConfigs = loadTopLevelConfig(HOPTIMATOR_CONFIG_MAP);
    Properties p = new Properties();
    p.putAll(topLevelConfigs);
    for (String expansionField : expansionFields) {
      if (topLevelConfigs == null || !topLevelConfigs.containsKey(expansionField)) {
        log.warn("Config map does not contain {}", expansionField);
        continue;
      }
      p.load(new StringReader(topLevelConfigs.get(expansionField)));
    }
    return p;
  }

  // Load top-level config map properties
  private Map<String, String> loadTopLevelConfig(String configMapName) throws Exception {
    K8sApi<V1ConfigMap, V1ConfigMapList> configMapApi = new K8sApi<>(K8sContext.currentContext(), K8sApiEndpoints.CONFIG_MAPS);
    return configMapApi.get(configMapName).getData();
  }
}
