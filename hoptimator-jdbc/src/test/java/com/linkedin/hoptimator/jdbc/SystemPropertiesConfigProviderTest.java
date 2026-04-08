package com.linkedin.hoptimator.jdbc;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;


class SystemPropertiesConfigProviderTest {

  @Test
  void testLoadConfigReturnsSystemProperties() {
    SystemPropertiesConfigProvider provider = new SystemPropertiesConfigProvider();

    Properties properties = provider.loadConfig(null);

    assertNotNull(properties);
    // System properties always contain java.version
    assertTrue(properties.containsKey("java.version"));
  }

  @Test
  void testLoadConfigReturnsSameAsSystemGetProperties() {
    SystemPropertiesConfigProvider provider = new SystemPropertiesConfigProvider();

    Properties properties = provider.loadConfig(null);

    // Should be the same reference as System.getProperties()
    assertSame(System.getProperties(), properties);
  }
}
