package com.linkedin.hoptimator.catalog;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

import java.util.function.Function;

public class ResourceTest {

  @Test
  public void handlesChainedEnvironments() {
    Resource.Environment env = new Resource.SimpleEnvironment() {{
      export("one", "1");
      export("foo", "bar");
    }}.orElse(new Resource.SimpleEnvironment() {{
      export("two", "2");
      export("foo", "car");
    }}.orElse(new Resource.SimpleEnvironment() {{
      export("three", "3");
      export("foo", "dar");
    }}));
    assertEquals("1", env.getOrDefault("one", () -> "x"));
    assertEquals("2", env.getOrDefault("two", () -> "x"));
    assertEquals("3", env.getOrDefault("three", () -> "x"));
    assertEquals("bar", env.getOrDefault("foo", () -> "x"));
    assertEquals("x", env.getOrDefault("oof", () -> "x"));
  }

  @Test
  public void rendersTemplates() {
    Resource.Environment env = new Resource.SimpleEnvironment() {{
      export("one", "1");
      export("foo", "bar");
    }};
    Resource res = new Resource("x") {{
      export("car", "Hyundai Accent");
      export("parts", "wheels\nseats\nbrakes\nwipers");
    }};

    Function<String, Resource.Template> f = x -> new Resource.SimpleTemplate(env, x);
    assertEquals("xyz", f.apply("xyz").render(res));
    assertEquals("bar", f.apply("{{foo}}").render(res));
    assertEquals("bar", f.apply("{{ foo }}").render(res));
    assertEquals("abc", f.apply("{{xyz:abc}}").render(res));
    assertEquals("hyundai-accent", f.apply("{{car toName}}").render(res));
    assertEquals("HYUNDAI-ACCENT", f.apply("{{car toName toUpperCase}}").render(res));
    assertEquals("WHEELSSEATSBRAKESWIPERS", f.apply("{{parts concat toUpperCase}}").render(res));
    assertEquals("BAR\nbar\nxyz", f.apply("{{foo toUpperCase}}\n{{foo toLowerCase}}\n{{x:xyz}}").render(res));
  }
}
