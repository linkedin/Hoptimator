package com.linkedin.hoptimator.util;

import java.io.StringReader;
import java.util.Properties;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.ConfigProvider;

public final class ConfigService {

  private static final Logger log = LoggerFactory.getLogger(ConfigService.class);

  private ConfigService() {
  }

  // Null namespace will default to current namespace, may not be used by some ConfigProviders.
  // loadTopLevelConfigs=true loads top level configs and expands input fields as file-like properties
  // loadTopLevelConfigs=false will only expand input fields as file-like properties
  // Ex:
  //  log.properties: |
  //    level=INFO
  public static Properties config(Properties connectionProperties, boolean loadTopLevelConfigs, String... expansionFields) {
    ServiceLoader<ConfigProvider> loader = ServiceLoader.load(ConfigProvider.class);
    Properties properties = new Properties();
    for (ConfigProvider provider : loader) {
      try {
        Properties loadedProperties = provider.loadConfig(connectionProperties);
        if (loadTopLevelConfigs) {
          log.debug("Loaded properties={} from provider={}", loadedProperties, provider);
          properties.putAll(loadedProperties);
        }
        for (String expansionField : expansionFields) {
          if (loadedProperties == null || !loadedProperties.containsKey(expansionField)) {
            log.warn("provider={} does not contain field={}", provider, expansionField);
            continue;
          }
          properties.load(new StringReader(loadedProperties.getProperty(expansionField)));
        }
      } catch (Exception e) {
        log.warn("Could not load properties for provider={}", provider, e);
      }
    }
    return properties;
  }

  public static Properties config(Properties connectionProperties, String... expansionFields) {
    return config(connectionProperties, true, expansionFields);
  }
}
