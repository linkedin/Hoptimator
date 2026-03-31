package com.linkedin.hoptimator.util;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class ConfigServiceTest {

  @Mock
  private Connection mockConnection;

  @BeforeEach
  void setUp() {
    TestConfigProvider.reset();
  }

  @AfterEach
  void tearDown() {
    TestConfigProvider.reset();
  }

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

  // properties.putAll(loadedProperties) is called when loadTopLevelConfigs=true, the provider's top-level properties
  // must appear in the result.

  @Test
  void testLoadTopLevelConfigsTrueIncludesProviderKeys() {
    TestConfigProvider.put("kafka.bootstrap.servers", "localhost:9092");
    TestConfigProvider.put("other.key", "other.value");

    Properties result = ConfigService.config(mockConnection, true);

    assertTrue(result.containsKey("kafka.bootstrap.servers"),
        "Top-level provider key must be present when loadTopLevelConfigs=true. Got: " + result);
    assertEquals("localhost:9092", result.getProperty("kafka.bootstrap.servers"));
    assertTrue(result.containsKey("other.key"),
        "All provider keys must be present when loadTopLevelConfigs=true. Got: " + result);
  }

  // When loadTopLevelConfigs=false, top-level keys must NOT appear.

  @Test
  void testLoadTopLevelConfigsFalseExcludesTopLevelKeys() {
    TestConfigProvider.put("top.level.key", "topValue");
    // No expansion field exists in the provider, so nothing extra is loaded.
    // The only way "top.level.key" ends up in result is if putAll() was called.

    Properties result = ConfigService.config(mockConnection, false);

    assertFalse(result.containsKey("top.level.key"),
        "Top-level key must NOT be present when loadTopLevelConfigs=false. Got: " + result);
  }

  // When the expansion field IS present in provider properties, its contents
  // must be expanded as file-like properties.
  @Test
  void testExpansionFieldIsExpandedWhenPresent() {
    // Put a multi-line properties block as the value of "my.expansion.field"
    TestConfigProvider.put("my.expansion.field", "expanded.key=expandedValue\nother.expanded=yes");

    // Ask ConfigService to expand "my.expansion.field"
    Properties result = ConfigService.config(mockConnection, false, "my.expansion.field");

    assertTrue(result.containsKey("expanded.key"),
        "Expanded key must be present. Got: " + result);
    assertEquals("expandedValue", result.getProperty("expanded.key"));
    assertTrue(result.containsKey("other.expanded"),
        "Second expanded key must be present. Got: " + result);
  }

  @Test
  void testExpansionFieldMissingFromProviderIsSkipped() {
    // Provider does NOT have the expansion field → should not crash, result is empty
    TestConfigProvider.put("some.other.key", "value");

    Properties result = ConfigService.config(mockConnection, false, "nonexistent.field");

    // Verify nothing was erroneously added from the expansion attempt
    assertNull(result.getProperty("some.other.key"),
        "Top-level key must not leak when loadTopLevelConfigs=false. Got: " + result);
  }
}
