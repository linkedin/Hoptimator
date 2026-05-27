package com.linkedin.hoptimator.util;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


class TemplateTest {

  // --- SimpleTemplate rendering tests ---

  @Test
  void testRenderSimpleVariable() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("name", "world");
    Template template = new Template.SimpleTemplate("Hello {{name}}!");

    String result = template.render(env);

    assertEquals("Hello world!", result);
  }

  @Test
  void testRenderMultipleVariables() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("first", "John")
        .with("last", "Doe");
    Template template = new Template.SimpleTemplate("{{first}} {{last}}");

    String result = template.render(env);

    assertEquals("John Doe", result);
  }

  @Test
  void testRenderWithDefaultValue() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment();
    Template template = new Template.SimpleTemplate("val={{key:fallback}}");

    String result = template.render(env);

    assertEquals("val=fallback", result);
  }

  @Test
  void testRenderWithDefaultValueOverriddenByEnv() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("key", "actual");
    Template template = new Template.SimpleTemplate("val={{key:fallback}}");

    String result = template.render(env);

    assertEquals("val=actual", result);
  }

  @Test
  void testRenderMissingVariableReturnsNull() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment();
    Template template = new Template.SimpleTemplate("Hello {{missing}}!");

    String result = template.render(env);

    assertNull(result);
  }

  @Test
  void testRenderToLowerCaseTransform() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("val", "HELLO");
    Template template = new Template.SimpleTemplate("{{val toLowerCase}}");

    String result = template.render(env);

    assertEquals("hello", result);
  }

  @Test
  void testRenderToUpperCaseTransform() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("val", "hello");
    Template template = new Template.SimpleTemplate("{{val toUpperCase}}");

    String result = template.render(env);

    assertEquals("HELLO", result);
  }

  @Test
  void testRenderConcatTransform() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("val", "line1\nline2");
    Template template = new Template.SimpleTemplate("{{val concat}}");

    String result = template.render(env);

    assertEquals("line1line2", result);
  }

  @Test
  void testRenderChainedTransforms() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("val", "hello\nworld");
    Template template = new Template.SimpleTemplate("{{val concat toUpperCase}}");

    String result = template.render(env);

    assertEquals("HELLOWORLD", result);
  }

  @Test
  void testRenderEqualConditionMatch() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("mode", "debug");
    Template template = new Template.SimpleTemplate("prefix{{mode==debug}}suffix");

    String result = template.render(env);

    // When condition matches, the placeholder is replaced with empty string
    assertEquals("prefixsuffix", result);
  }

  @Test
  void testRenderEqualConditionNoMatch() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("mode", "release");
    Template template = new Template.SimpleTemplate("prefix{{mode==debug}}suffix");

    String result = template.render(env);

    // When condition does not match, the template returns null
    assertNull(result);
  }

  @Test
  void testRenderNotEqualConditionMatch() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("mode", "release");
    Template template = new Template.SimpleTemplate("prefix{{mode!=debug}}suffix");

    String result = template.render(env);

    // When != condition matches (mode is not "debug"), placeholder becomes empty
    assertEquals("prefixsuffix", result);
  }

  @Test
  void testRenderNotEqualConditionNoMatch() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("mode", "debug");
    Template template = new Template.SimpleTemplate("prefix{{mode!=debug}}suffix");

    String result = template.render(env);

    // When != condition does not match (mode IS "debug"), template returns null
    assertNull(result);
  }

  @Test
  void testRenderMultilineInListContext() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("items", "item1\nitem2\nitem3");
    Template template = new Template.SimpleTemplate("  - {{items}}");

    String result = template.render(env);

    assertNotNull(result);
    // Prefix "  - " replaces newlines within the value
    assertEquals("  - item1  - item2  - item3", result);
  }

  @Test
  void testRenderNoTemplateVariables() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment();
    Template template = new Template.SimpleTemplate("plain text");

    String result = template.render(env);

    assertEquals("plain text", result);
  }

  @Test
  void testRenderWithDefaultTransform() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment();
    Template template = new Template.SimpleTemplate("{{key:default_val toUpperCase}}");

    String result = template.render(env);

    assertEquals("DEFAULT_VAL", result);
  }

  @Test
  void testRenderWithDotInVariableName() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("app.name", "myApp");
    Template template = new Template.SimpleTemplate("name={{app.name}}");

    String result = template.render(env);

    assertEquals("name=myApp", result);
  }

  @Test
  void testRenderWithHyphenInVariableName() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("my-var", "value");
    Template template = new Template.SimpleTemplate("v={{my-var}}");

    String result = template.render(env);

    assertEquals("v=value", result);
  }

  @Test
  void testRenderWithUnderscoreInVariableName() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment().with("my_var", "value");
    Template template = new Template.SimpleTemplate("v={{my_var}}");

    String result = template.render(env);

    assertEquals("v=value", result);
  }

  // --- SimpleEnvironment constructor tests ---

  @Test
  void testSimpleEnvironmentConstructorWithVars() throws SQLException {
    Template.SimpleEnvironment env = new Template.SimpleEnvironment()
        .with("k", "v");

    assertEquals("v", env.getOrDefault("k", () -> null));
  }

  // --- Environment constants ---

  @Test
  void testEnvironmentProcessNotNull() {
    assertNotNull(Template.Environment.PROCESS);
  }

  @Test
  void testGetOrDefaultReturnsEnvValueWhenKeyPresent() throws SQLException {
    Template.SimpleEnvironment env = new Template.SimpleEnvironment().with("myKey", "envValue");

    String result = env.getOrDefault("myKey", () -> "defaultValue");

    // Returns the default
    assertEquals("envValue", result,
        "Should return env value when key is present, not the default");
  }

  @Test
  void testGetOrDefaultReturnsDefaultWhenKeyAbsent() throws SQLException {
    Template.SimpleEnvironment env = new Template.SimpleEnvironment();

    String result = env.getOrDefault("absent", () -> "theDefault");

    assertEquals("theDefault", result,
        "Should return the default supplier value when key is absent");
  }

  @Test
  void testGetOrDefaultReturnsDefaultWhenSupplierThrows() throws SQLException {
    // Key present but supplier throws → fallback default is returned
    Template.SimpleEnvironment env = new Template.SimpleEnvironment()
        .with("badKey", () -> {
          throw new RuntimeException("forced failure");
        });

    String result = env.getOrDefault("badKey", () -> "safeDefault");

    assertEquals("safeDefault", result,
        "Should return default when the registered supplier throws");
  }

  @Test
  void testGetOrDefaultEnvValueIsDistinctFromDefault() throws SQLException {
    // Verify env value and default are different so neither assertion is trivially true
    Template.SimpleEnvironment env = new Template.SimpleEnvironment()
        .with("k", "envResult");

    String result = env.getOrDefault("k", () -> "otherResult");

    assertNotNull(result);
    assertEquals("envResult", result);
    assertNotEquals("otherResult", result,
        "env value and default must differ for the test to be meaningful");
  }

  @Test
  void testDummyEnvironmentReturnsDefaultWhenSupplierProvides() throws SQLException {
    Template.DummyEnvironment env = new Template.DummyEnvironment();

    String result = env.getOrDefault("anyKey", () -> "suppliedDefault");

    assertEquals("suppliedDefault", result,
        "DummyEnvironment should return non-null supplier result");
  }

  @Test
  void testDummyEnvironmentReturnsPlaceholderWhenSupplierReturnsNull() throws SQLException {
    Template.DummyEnvironment env = new Template.DummyEnvironment();

    String result = env.getOrDefault("myVar", () -> null);

    assertEquals("{{myVar}}", result,
        "DummyEnvironment should return placeholder when supplier returns null");
  }

  @Test
  void testDummyEnvironmentPlaceholderAndDefaultAreDistinct() throws SQLException {
    // Ensure the two branches in DummyEnvironment.getOrDefault produce different results
    Template.DummyEnvironment env = new Template.DummyEnvironment();

    String withNull    = env.getOrDefault("x", () -> null);
    String withDefault = env.getOrDefault("x", () -> "real");

    assertEquals("{{x}}", withNull);
    assertEquals("real",   withDefault);
    assertNotEquals(withNull, withDefault,
        "Placeholder branch and default branch must return different values");
  }

  @Test
  void testConditionalDefaultValueIsUsedWhenKeyMissing() throws SQLException {
    // {{key:default}} — when key is absent, default text is rendered
    Template.Environment env = new Template.SimpleEnvironment();
    Template template = new Template.SimpleTemplate("result={{missing:fallbackText}}");

    String result = template.render(env);

    assertEquals("result=fallbackText", result,
        "When key is absent, the default from the template should be used");
  }

  @Test
  void testConditionalDefaultValueIsOverriddenByEnvKey() throws SQLException {
    // {{key:default}} — when key IS present, env value overrides the default
    Template.Environment env = new Template.SimpleEnvironment().with("myKey", "envOverride");
    Template template = new Template.SimpleTemplate("result={{myKey:defaultText}}");

    String result = template.render(env);

    assertEquals("result=envOverride", result,
        "When key is present, the env value should override the template default");
  }

  @Test
  void testRender() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("name", "name")
        .with("nameUpper", "name")
        .with("nameLower", "NAME")
        .with("multiline", "1\n2\n3\n")
        .with("multilineUpper", "a\nb\nc\n")
        .with("other", "test")
        .with("supplier", () -> "value");

    String template = "{{keys:KEY}}\n"
        + "{{keyPrefix:}}\n"
        + "{{name:default}}\n"
        + "{{nameUpper toUpperCase}}\n"
        + "{{nameLower toLowerCase}}\n"
        + "{{multiline concat}}\n"
        + "{{multilineUpper concat toUpperCase}}\n"
        + "{{other unknown}}\n"
        + "{{supplier}}\n";

    String renderedTemplate = new Template.SimpleTemplate(template).render(env);
    List<String> renderedTemplates = Arrays.asList(renderedTemplate.split("\n"));
    assertEquals(9, renderedTemplates.size());
    assertEquals("KEY", renderedTemplates.get(0));
    assertEquals("", renderedTemplates.get(1));
    assertEquals("name", renderedTemplates.get(2));
    assertEquals("NAME", renderedTemplates.get(3));
    assertEquals("name", renderedTemplates.get(4));
    assertEquals("123", renderedTemplates.get(5));
    assertEquals("ABC", renderedTemplates.get(6));
    assertEquals("test", renderedTemplates.get(7));
    assertEquals("value", renderedTemplates.get(8));
  }

  @Test
  void templateEquality() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("field1", "true");
    String template = "{{name:default}}\n"
        + "{{field1==true}}\n"
        + "{{field1!=false}}\n";

    String renderedTemplate = new Template.SimpleTemplate(template).render(env);
    List<String> renderedTemplates = Arrays.asList(renderedTemplate.split("\n"));
    assertEquals(1, renderedTemplates.size());
    assertEquals("default", renderedTemplates.get(0));
  }

  @Test
  void templateEqualityFailure() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("field1", "true");
    String template1 = "{{name:default}}\n"
        + "{{field1==false}}\n";

    String renderedTemplate = new Template.SimpleTemplate(template1).render(env);
    assertNull(renderedTemplate);

    String template2 = "{{name:default}}\n"
        + "{{field1!=true}}\n";
    renderedTemplate = new Template.SimpleTemplate(template2).render(env);
    assertNull(renderedTemplate);
  }

  @Test
  void supplierException() {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("field", () -> {
          throw new SQLException("test");
        });
    String template = "{{field}}";

    assertThrows(SQLException.class, () -> new Template.SimpleTemplate(template).render(env));
  }

}
