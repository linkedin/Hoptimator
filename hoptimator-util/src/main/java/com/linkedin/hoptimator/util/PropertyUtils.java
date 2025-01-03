package com.linkedin.hoptimator.util;

import java.util.Properties;

public final class PropertyUtils {

  public static final String HOPTIMATOR_CONFIG_DOMAIN = "hoptimator.config";

  private PropertyUtils() {
  }

  public static Properties getDomainProperties(Properties properties, String prefix) {
    return getDomainProperties(properties, prefix, false);
  }

  public static Properties getDomainProperties(Properties properties, String prefix, boolean preserveFullKey) {
    String fullPrefix;
    if (prefix == null || prefix.isEmpty()) {
      fullPrefix = "";
    } else {
      fullPrefix = prefix.endsWith(".") ? prefix : prefix + ".";
    }

    Properties ret = new Properties();
    properties.keySet().forEach((key) -> {
      String keyStr = key.toString();
      if (keyStr.startsWith(fullPrefix) && !keyStr.equals(fullPrefix)) {
        if (preserveFullKey) {
          ret.put(keyStr, properties.getProperty(keyStr));
        } else {
          ret.put(keyStr.substring(fullPrefix.length()), properties.getProperty(keyStr));
        }
      }
    });
    return ret;
  }
}
