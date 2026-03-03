package com.linkedin.hoptimator.jdbc;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for DeployerUtils utility methods.
 */
class DeployerUtilsTest {

  // --- parseIntOption tests ---

  @Test
  void testParseIntOptionReturnsDefault() {
    Map<String, String> options = Collections.emptyMap();
    assertEquals(10, DeployerUtils.parseIntOption(options, "partitions", 10));
  }

  @Test
  void testParseIntOptionReturnsCustomValue() {
    Map<String, String> options = Map.of("partitions", "32");
    assertEquals(32, DeployerUtils.parseIntOption(options, "partitions", 10));
  }

  @Test
  void testParseIntOptionReturnsDefaultForInvalidValue() {
    Map<String, String> options = Map.of("partitions", "not-a-number");
    assertEquals(10, DeployerUtils.parseIntOption(options, "partitions", 10));
  }

  @Test
  void testParseIntOptionWithNullDefault() {
    Map<String, String> options = Collections.emptyMap();
    Integer result = DeployerUtils.parseIntOption(options, "nonexistent", null);
    assertNull(result);
  }

  @Test
  void testParseIntOptionWithNegativeValue() {
    Map<String, String> options = Map.of("partitions", "-5");
    assertEquals(-5, DeployerUtils.parseIntOption(options, "partitions", 10));
  }

  // --- parseLongOption tests ---

  @Test
  void testParseLongOptionReturnsNullDefault() {
    Map<String, String> options = Collections.emptyMap();
    assertNull(DeployerUtils.parseLongOption(options, "retention", null));
  }

  @Test
  void testParseLongOptionReturnsCustomValue() {
    Map<String, String> options = Map.of("retention", "604800000");
    assertEquals(604800000L, DeployerUtils.parseLongOption(options, "retention", null));
  }

  @Test
  void testParseLongOptionReturnsDefaultForInvalidValue() {
    Map<String, String> options = Map.of("retention", "not-a-number");
    assertEquals(604800000L, DeployerUtils.parseLongOption(options, "retention", 604800000L));
  }

  // --- parseBooleanOption tests ---

  @Test
  void testParseBooleanOptionReturnsDefault() {
    Map<String, String> options = Collections.emptyMap();
    assertEquals(true, DeployerUtils.parseBooleanOption(options, "enabled", true));
  }

  @Test
  void testParseBooleanOptionReturnsTrue() {
    Map<String, String> options = Map.of("enabled", "true");
    assertEquals(true, DeployerUtils.parseBooleanOption(options, "enabled", false));
  }

  @Test
  void testParseBooleanOptionReturnsFalse() {
    Map<String, String> options = Map.of("enabled", "false");
    assertEquals(false, DeployerUtils.parseBooleanOption(options, "enabled", true));
  }

  @Test
  void testParseBooleanOptionWithNullDefault() {
    Map<String, String> options = Collections.emptyMap();
    assertNull(DeployerUtils.parseBooleanOption(options, "nonexistent", null));
  }

  // --- parseDoubleOption tests ---

  @Test
  void testParseDoubleOptionReturnsDefault() {
    Map<String, String> options = Collections.emptyMap();
    assertEquals(1.5, DeployerUtils.parseDoubleOption(options, "threshold", 1.5));
  }

  @Test
  void testParseDoubleOptionReturnsCustomValue() {
    Map<String, String> options = Map.of("threshold", "2.75");
    assertEquals(2.75, DeployerUtils.parseDoubleOption(options, "threshold", 1.5));
  }

  @Test
  void testParseDoubleOptionReturnsDefaultForInvalidValue() {
    Map<String, String> options = Map.of("threshold", "not-a-number");
    assertEquals(1.5, DeployerUtils.parseDoubleOption(options, "threshold", 1.5));
  }

  @Test
  void testParseDoubleOptionWithNullDefault() {
    Map<String, String> options = Collections.emptyMap();
    assertNull(DeployerUtils.parseDoubleOption(options, "nonexistent", null));
  }

  @Test
  void testParseDoubleOptionWithNegativeValue() {
    Map<String, String> options = Map.of("threshold", "-3.14");
    assertEquals(-3.14, DeployerUtils.parseDoubleOption(options, "threshold", 1.5));
  }
}
