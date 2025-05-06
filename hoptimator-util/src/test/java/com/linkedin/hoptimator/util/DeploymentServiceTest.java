package com.linkedin.hoptimator.util;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import static com.linkedin.hoptimator.util.DeploymentService.HINT_OPTION;
import static com.linkedin.hoptimator.util.DeploymentService.PIPELINE_OPTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class DeploymentServiceTest {

  /**
   * "hint" keys <b>and</b> values are required to be non-{@code null}. An
   * empty {@link Map} is considered invalid and should <b>not</b> be added
   * to the {@link Properties} object.
   * <p>
   * nb. "pipeline" values are <b>always</b> added when present.
   */
  @Test
  void parseHints() {
    Map<String, String> empty = DeploymentService.parseHints(new Properties());
    assertTrue(empty.isEmpty(), "An empty map should not add `hints`.");

    Map<String, String> defined = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "key=value");
    }});
    assertEquals("value", defined.get("key"), "Did not match expected key value pair: `key=value`.");

    Map<String, String> nokey = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "flag0=,flag1=");
    }});
    assertTrue(nokey.keySet().containsAll(Arrays.asList("flag0", "flag1")), "Expected to find flags.");

    Map<String, String> pipelineOnly = DeploymentService.parseHints(new Properties() {{
      put(PIPELINE_OPTION, "pipeline");
    }});
    assertEquals("pipeline", pipelineOnly.get(PIPELINE_OPTION), "Did not match expected `pipeline` value.");

    Map<String, String> both = DeploymentService.parseHints(new Properties() {{
      putAll(defined);
      put(PIPELINE_OPTION, "pipeline");
    }});
    assertEquals("pipeline", both.get(PIPELINE_OPTION), "Did not match expected `pipeline` value.");
  }
}