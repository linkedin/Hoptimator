package com.linkedin.hoptimator.catalog;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

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
}
