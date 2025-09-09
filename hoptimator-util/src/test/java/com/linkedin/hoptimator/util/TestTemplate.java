package com.linkedin.hoptimator.util;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TestTemplate {

  @Test
  public void testRender() throws SQLException {
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
        + "{{missing notPresent}}\n"
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
  public void missingProperty() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment();
    String template = "{{field}}";
    String renderedTemplate = new Template.SimpleTemplate(template).render(env);
    assertNull(renderedTemplate);
  }

  @Test
  public void ignoreTemplateProperty() throws SQLException {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("field", "true");
    String template = "{{field notPresent}}";

    String renderedTemplate = new Template.SimpleTemplate(template).render(env);
    assertNull(renderedTemplate);
  }

  @Test
  public void supplierException() {
    Template.Environment env = new Template.SimpleEnvironment()
        .with("field", () -> {
          throw new SQLException("test");
        });
    String template = "{{field}}";

    assertThrows(SQLException.class, () -> {
      new Template.SimpleTemplate(template).render(env);
    });
  }
}
