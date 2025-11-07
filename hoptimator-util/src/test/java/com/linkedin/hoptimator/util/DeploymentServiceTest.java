package com.linkedin.hoptimator.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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

  /**
   * Test that URL-encoded values are properly decoded, allowing commas and other special characters.
   */
  @Test
  void parseHintsWithUrlEncodedValues() {
    // Test value with commas (URL-encoded)
    String valueWithCommas = "host1:9092,host2:9092,host3:9092";
    String encoded = URLEncoder.encode(valueWithCommas, StandardCharsets.UTF_8);
    Map<String, String> urlEncoded = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "servers=" + encoded + ",parallelism=4");
    }});
    assertEquals(valueWithCommas, urlEncoded.get("servers"),
        "URL-encoded value with commas should be decoded correctly");
    assertEquals("4", urlEncoded.get("parallelism"));

    // Test complex nested configuration (URL-encoded)
    String complexValue = "bootstrap.servers=host1:9092,host2:9092,host3:9092";
    String encodedComplex = URLEncoder.encode(complexValue, StandardCharsets.UTF_8);
    Map<String, String> complexEncoded = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "kafka.config=" + encodedComplex + ",other=simple");
    }});
    assertEquals(complexValue, complexEncoded.get("kafka.config"),
        "Complex URL-encoded value should be decoded correctly");
    assertEquals("simple", complexEncoded.get("other"));

    // Test value with equals signs and commas (URL-encoded)
    String nestedKV = "key1=value1,key2=value2,key3=value3";
    String encodedNested = URLEncoder.encode(nestedKV, StandardCharsets.UTF_8);
    Map<String, String> nestedEncoded = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "nested.config=" + encodedNested);
    }});
    assertEquals(nestedKV, nestedEncoded.get("nested.config"),
        "Nested K=V pairs with commas should be preserved when URL-encoded");

    // Test standard values
    String simpleValue = "value";
    String encodedSimpleValue = URLEncoder.encode(simpleValue, StandardCharsets.UTF_8);
    Map<String, String> hintsSimpleValue = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "key=" + encodedSimpleValue);
    }});
    assertEquals(simpleValue, hintsSimpleValue.get("key"), "Simple K=V strings should be preserved when URL-encoded");
  }

  /**
   * Test backward compatibility - non-encoded values should still work.
   */
  @Test
  void parseHintsBackwardCompatibility() {
    // Test that existing non-encoded values still work
    Map<String, String> simple = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "kafka.partitions=4,flink.parallelism=2");
    }});
    assertEquals("4", simple.get("kafka.partitions"));
    assertEquals("2", simple.get("flink.parallelism"));

    // Test existing examples from README still work
    Map<String, String> readmeExample = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "kafka.partitions=4,flink.parallelism=2,kafka.source.k1=v1,kafka.sink.k2=v2");
    }});
    assertEquals("4", readmeExample.get("kafka.partitions"));
    assertEquals("2", readmeExample.get("flink.parallelism"));
    assertEquals("v1", readmeExample.get("kafka.source.k1"));
    assertEquals("v2", readmeExample.get("kafka.sink.k2"));

    // Test values with dots and underscores (common in property names)
    Map<String, String> dotted = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "kafka.source.properties.group.id=my_group,offline.table.name=ads_offline");
    }});
    assertEquals("my_group", dotted.get("kafka.source.properties.group.id"));
    assertEquals("ads_offline", dotted.get("offline.table.name"));
  }

  /**
   * Test mixed encoded and non-encoded values (should work together).
   */
  @Test
  void parseHintsMixedEncodedAndNonEncoded() {
    String encodedValue = URLEncoder.encode("value,with,commas", StandardCharsets.UTF_8);
    Map<String, String> mixed = DeploymentService.parseHints(new Properties() {{
      put(HINT_OPTION, "encoded=" + encodedValue + ",simple=value,another=test");
    }});
    assertEquals("value,with,commas", mixed.get("encoded"));
    assertEquals("value", mixed.get("simple"));
    assertEquals("test", mixed.get("another"));
  }
}