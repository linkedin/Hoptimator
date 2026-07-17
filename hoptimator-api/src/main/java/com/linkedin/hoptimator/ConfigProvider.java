package com.linkedin.hoptimator;

import java.util.Properties;

public interface ConfigProvider {

  Properties loadConfig(DeploymentContext context) throws Exception;
}
