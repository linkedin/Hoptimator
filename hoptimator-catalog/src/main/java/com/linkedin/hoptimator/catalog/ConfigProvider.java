package com.linkedin.hoptimator.catalog;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.function.Function;

/** Provides key-value properties, e.g. for connector configs. */
public interface ConfigProvider {

  /** Connector configuration for the given table */
  Map<String, String> config(String tableName);

  static ConfigProvider empty() {
    return x -> Collections.emptyMap();
  }

  static ConfigProvider from(Map<String, ?> configs) {
    Map<String, String> strings = configs.entrySet().stream()
      .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue().toString()));
    return x -> strings;
  }

  default ConfigProvider with(String key, Function<String, String> valueFunction) {
    return x -> {
      Map<String, String> base = config(x);
      if (base.containsKey(key)) {
        throw new IllegalStateException("Key '" + key + "' previously defined.");
      }
      Map<String, String> combined = new HashMap<>();
      combined.putAll(base);
      combined.put(key, valueFunction.apply(x));
      return combined;
    };
  }

  default ConfigProvider with(String key, String value) {
    return with(key, x -> value);
  }
}
