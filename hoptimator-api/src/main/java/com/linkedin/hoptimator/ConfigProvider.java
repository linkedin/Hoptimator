package com.linkedin.hoptimator;

import java.sql.Connection;
import java.util.Properties;

public interface ConfigProvider {

  Properties loadConfig(Connection connection) throws Exception;
}
