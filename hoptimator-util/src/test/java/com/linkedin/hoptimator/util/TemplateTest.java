package com.linkedin.hoptimator.util;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


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
}
