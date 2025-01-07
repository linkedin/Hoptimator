package com.linkedin.hoptimator.util;

import java.util.Properties;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.ConfigProvider;

public final class ConfigService {

  private static final Logger log = LoggerFactory.getLogger(ConfigService.class);

  private ConfigService() {
  }

  // A Prefix is appended to all system configs added to prevent collisions
  public static Properties config(String... expansionFields) {
    ServiceLoader<ConfigProvider> loader = ServiceLoader.load(ConfigProvider.class);
    Properties properties = new Properties();
    for (ConfigProvider provider : loader) {
      try {
        properties.putAll(provider.loadConfig(expansionFields));
      } catch (Exception e) {
        log.warn("Could not load config map for provider={}", provider, e);
      }
    }
    return properties;
  }
}
