package com.linkedin.hoptimator.util;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class TemplateEnvironmentTest {

  @Test
  void testDummyEnvironmentReturnsPlaceholderForMissingKeys() throws SQLException {
    Template.DummyEnvironment env = new Template.DummyEnvironment();

    String result = env.getOrDefault("missingKey", () -> null);

    assertEquals("{{missingKey}}", result);
  }

  @Test
  void testDummyEnvironmentReturnsDefaultWhenSupplierProvides() throws SQLException {
    Template.DummyEnvironment env = new Template.DummyEnvironment();

    String result = env.getOrDefault("key", () -> "defaultValue");

    assertEquals("defaultValue", result);
  }

  @Test
  void testProcessEnvironmentThrowsForMissingKey() {
    Template.ProcessEnvironment env = new Template.ProcessEnvironment();

    assertThrows(IllegalArgumentException.class,
        () -> env.getOrDefault("__NONEXISTENT_KEY_FOR_TEST__", () -> null));
  }

  @Test
  void testProcessEnvironmentUsesDefaultWhenMissing() throws SQLException {
    Template.ProcessEnvironment env = new Template.ProcessEnvironment();

    String result = env.getOrDefault("__NONEXISTENT_KEY_FOR_TEST__", () -> "fallback");

    assertEquals("fallback", result);
  }

  @Test
  void testSimpleEnvironmentWithMapValues() throws SQLException {
    Map<String, String> values = new HashMap<>();
    values.put("k1", "v1");
    values.put("k2", "v2");

    Template.SimpleEnvironment env = new Template.SimpleEnvironment().with(values);

    assertEquals("v1", env.getOrDefault("k1", () -> null));
    assertEquals("v2", env.getOrDefault("k2", () -> null));
  }

  @Test
  void testSimpleEnvironmentWithProperties() throws SQLException {
    Properties props = new Properties();
    props.setProperty("pk1", "pv1");

    Template.SimpleEnvironment env = new Template.SimpleEnvironment().with(props);

    assertEquals("pv1", env.getOrDefault("pk1", () -> null));
  }

  @Test
  void testSimpleEnvironmentWithKeyMapYamlFormat() throws SQLException {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("key", "value");

    Template.SimpleEnvironment env = new Template.SimpleEnvironment().with("config", configMap);

    String result = env.getOrDefault("config", () -> null);
    // YAML dump produces key-value format
    assertTrue(result.contains("key"));
    assertTrue(result.contains("value"));
  }

  @Test
  void testSimpleEnvironmentWithKeyPropertiesFormat() throws SQLException {
    Properties props = new Properties();
    props.setProperty("pk", "pv");

    Template.SimpleEnvironment env = new Template.SimpleEnvironment().with("propConfig", props);

    String result = env.getOrDefault("propConfig", () -> null);
    assertTrue(result.contains("pk"));
    assertTrue(result.contains("pv"));
  }

  @Test
  void testSimpleEnvironmentThrowsForMissingKey() {
    Template.SimpleEnvironment env = new Template.SimpleEnvironment();

    assertThrows(IllegalArgumentException.class,
        () -> env.getOrDefault("nonexistent", () -> null));
  }

  @Test
  void testSimpleEnvironmentUsesDefaultOnException() throws SQLException {
    Template.SimpleEnvironment env = new Template.SimpleEnvironment()
        .with("badKey", () -> {
          throw new RuntimeException("broken");
        });

    String result = env.getOrDefault("badKey", () -> "fallback");

    assertEquals("fallback", result);
  }

  @Test
  void testOrElseFallsBackToSecondEnvironment() throws SQLException {
    Template.SimpleEnvironment primary = new Template.SimpleEnvironment().with("a", "1");
    Template.SimpleEnvironment secondary = new Template.SimpleEnvironment().with("b", "2");

    Template.Environment combined = primary.orElse(secondary);

    assertEquals("1", combined.getOrDefault("a", () -> null));
    assertEquals("2", combined.getOrDefault("b", () -> null));
  }

  @Test
  void testOrIgnoreReturnsDummyPlaceholders() throws SQLException {
    Template.SimpleEnvironment env = new Template.SimpleEnvironment().with("a", "1");

    Template.Environment ignoring = env.orIgnore();

    assertEquals("1", ignoring.getOrDefault("a", () -> null));
    assertEquals("{{missing}}", ignoring.getOrDefault("missing", () -> null));
  }

  @Test
  void testEmptyEnvironmentConstant() throws SQLException {
    Template.Environment empty = Template.Environment.EMPTY;

    assertThrows(IllegalArgumentException.class,
        () -> empty.getOrDefault("anything", () -> null));
  }

  @Test
  void testSimpleTemplateRenderWithMultilineInListContext() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("items", "item1\nitem2\nitem3");

    String template = "  - {{items}}";
    String result = new Template.SimpleTemplate(template).render(env);

    // The prefix "  - " is applied to each line
    assertNotNull(result);
    assertTrue(result.contains("item1"));
    assertTrue(result.contains("item2"));
    assertTrue(result.contains("item3"));
  }

  private static void assertTrue(boolean condition) {
    Assertions.assertTrue(condition);
  }

  private static void assertNotNull(Object obj) {
    Assertions.assertNotNull(obj);
  }
}
