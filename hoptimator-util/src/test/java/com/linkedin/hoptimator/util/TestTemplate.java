package com.linkedin.hoptimator.util;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTemplate {

  @Test
  public void testRender() {
    Template.Environment env =
        Template.Environment.EMPTY
            .with("name", "name")
            .with("nameUpper", "name")
            .with("nameLower", "NAME")
            .with("multiline", "1\n2\n3\n")
            .with("multilineUpper", "a\nb\nc\n")
            .with("other", "test");

    String template = "{{keys:KEY}}\n"
        + "{{keyPrefix:}}\n"
        + "{{name:default}}\n"
        + "{{nameUpper toUpperCase}}\n"
        + "{{nameLower toLowerCase}}\n"
        + "{{multiline concat}}\n"
        + "{{multilineUpper concat toUpperCase}}\n"
        + "{{other unknown}}\n";

    String renderedTemplate = new Template.SimpleTemplate(template).render(env);
    List<String> renderedTemplates = Arrays.asList(renderedTemplate.split("\n"));
    assertEquals(8, renderedTemplates.size());
    assertEquals("KEY", renderedTemplates.get(0));
    assertEquals("", renderedTemplates.get(1));
    assertEquals("name", renderedTemplates.get(2));
    assertEquals("NAME", renderedTemplates.get(3));
    assertEquals("name", renderedTemplates.get(4));
    assertEquals("123", renderedTemplates.get(5));
    assertEquals("ABC", renderedTemplates.get(6));
    assertEquals("test", renderedTemplates.get(7));
  }
}
