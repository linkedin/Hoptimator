package com.linkedin.hoptimator.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;


@ExtendWith(MockitoExtension.class)
class ConfigServiceTest {

  @Mock
  private Connection mockConnection;

  @Test
  void testConfigReturnsPropertiesEvenWithNoProviders() {
    // With no ServiceLoader providers registered, should return empty properties
    Properties result = ConfigService.config(mockConnection);

    assertNotNull(result);
  }

  @Test
  void testConfigWithExpansionFieldsReturnsProperties() {
    Properties result = ConfigService.config(mockConnection, "log.properties");

    assertNotNull(result);
  }

  @Test
  void testConfigWithLoadTopLevelConfigsFalse() {
    Properties result = ConfigService.config(mockConnection, false, "field1");

    assertNotNull(result);
  }
}
