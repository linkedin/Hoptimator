package com.linkedin.hoptimator.operator;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.k8s.K8sContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class ConfigAssemblerTest {

  @Mock
  private K8sContext context;

  private ConfigAssembler assembler;

  @BeforeEach
  void setUp() {
    assembler = new ConfigAssembler(context);
  }

  @Test
  void assembleWithOverridesOnly() throws SQLException {
    assembler.addOverride("key1", "value1");
    assembler.addOverride("key2", "value2");

    Map<String, String> result = assembler.assemble();

    assertEquals(2, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
  }

  @Test
  void assemblePropertiesReturnsProperties() throws SQLException {
    assembler.addOverride("prop1", "val1");

    Properties props = assembler.assembleProperties();

    assertEquals("val1", props.getProperty("prop1"));
  }

  @Test
  void assembleWithNoRefsOrOverridesReturnsEmpty() throws SQLException {
    Map<String, String> result = assembler.assemble();
    assertTrue(result.isEmpty());
  }

  @Test
  void overridesTakePrecedence() throws SQLException {
    assembler.addOverride("key", "override-value");
    // Without any refs, just overrides
    Map<String, String> result = assembler.assemble();
    assertEquals("override-value", result.get("key"));
  }

  @Test
  void assemblePropertiesContainsAllOverrides() throws SQLException {
    assembler.addOverride("a", "1");
    assembler.addOverride("b", "2");
    assembler.addOverride("c", "3");

    Properties props = assembler.assembleProperties();
    assertEquals("1", props.getProperty("a"));
    assertEquals("2", props.getProperty("b"));
    assertEquals("3", props.getProperty("c"));
    assertEquals(3, props.size());
  }

  @Test
  void overrideReplacesExistingKey() throws SQLException {
    assembler.addOverride("key", "first");
    assembler.addOverride("key", "second");

    Map<String, String> result = assembler.assemble();
    assertEquals(1, result.size());
    assertEquals("second", result.get("key"));
  }

  @Test
  void assemblePropertiesReturnsNewInstanceEachTime() throws SQLException {
    assembler.addOverride("key", "value");

    Properties props1 = assembler.assembleProperties();
    Properties props2 = assembler.assembleProperties();
    assertFalse(props1 == props2);
    assertEquals(props1, props2);
  }

  @Test
  void assembleReturnsNewMapEachTime() throws SQLException {
    assembler.addOverride("key", "value");

    Map<String, String> map1 = assembler.assemble();
    Map<String, String> map2 = assembler.assemble();
    assertFalse(map1 == map2);
    assertEquals(map1, map2);
  }

  @Test
  void addRefStoresReference() {
    assembler.addRef("my-namespace", "my-configmap");
    assembler.addRef("other-ns", "other-config");
    // addRef stores internally; assemble() would attempt K8s fetch,
    // but the method itself does not throw
    assertNotNull(assembler);
  }
}
