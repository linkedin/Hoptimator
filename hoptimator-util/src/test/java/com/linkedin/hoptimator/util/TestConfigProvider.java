package com.linkedin.hoptimator.util;

import com.linkedin.hoptimator.ConfigProvider;

import java.sql.Connection;
import java.util.Properties;


/**
 * Test-only ConfigProvider registered via ServiceLoader to allow
 * tests for ConfigService.config() branches that only execute when providers exist.
 */
public class TestConfigProvider implements ConfigProvider {

  /** Properties loaded by this provider — populated by each test via static state. */
  static final Properties PROPERTIES = new Properties();

  /**
   * Reset to a clean state before each test. Call this in test setup.
   */
  public static void reset() {
    PROPERTIES.clear();
  }

  /**
   * Populate the properties this provider returns.
   */
  public static void put(String key, String value) {
    PROPERTIES.setProperty(key, value);
  }

  @Override
  public Properties loadConfig(Connection connection) {
    Properties copy = new Properties();
    copy.putAll(PROPERTIES);
    return copy;
  }
}
