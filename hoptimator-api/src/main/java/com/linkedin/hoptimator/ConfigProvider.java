package com.linkedin.hoptimator;

import java.util.Map;

public interface ConfigProvider {

  Map<String, String> loadConfig() throws Exception;
}
