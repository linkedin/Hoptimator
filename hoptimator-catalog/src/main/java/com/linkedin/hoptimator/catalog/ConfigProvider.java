package com.linkedin.hoptimator.catalog;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
    if (configs == null) {
      return empty();
    } else {
      return x -> configs.entrySet().stream()
        .collect(Collectors.toMap(y -> y.getKey(), y -> y.getValue().toString()));
    }
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

  default ConfigProvider with(String key, Integer value) {
    return with(key, x -> Optional.ofNullable(value).map(y -> Integer.toString(y)).orElse(null));
  }

  default ConfigProvider withPrefix(String prefix) {
    return x -> config(x).entrySet().stream()
      .collect(Collectors.toMap(y -> prefix + y.getKey(), y -> y.getValue()));
  }
}
