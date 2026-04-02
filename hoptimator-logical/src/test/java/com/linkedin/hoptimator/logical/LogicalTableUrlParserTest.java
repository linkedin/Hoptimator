package com.linkedin.hoptimator.logical;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class LogicalTableUrlParserTest {

  @Test
  public void testTwoTierUrl() {
    LogicalTableUrlParser parser =
        new LogicalTableUrlParser("jdbc:logical://nearline=xinfra-tracking;online=venice");

    Map<String, String> tiers = parser.tiers();
    assertThat(tiers).hasSize(2);
    assertThat(tiers).containsEntry("nearline", "xinfra-tracking");
    assertThat(tiers).containsEntry("online", "venice");
    assertThat(parser.pipelineOverrides()).isEmpty();
  }

  @Test
  public void testThreeTierUrl() {
    LogicalTableUrlParser parser = new LogicalTableUrlParser(
        "jdbc:logical://nearline=xinfra-tracking;offline=openhouse;online=venice");

    Map<String, String> tiers = parser.tiers();
    assertThat(tiers).hasSize(3);
    assertThat(tiers).containsEntry("nearline", "xinfra-tracking");
    assertThat(tiers).containsEntry("offline", "openhouse");
    assertThat(tiers).containsEntry("online", "venice");
    assertThat(parser.pipelineOverrides()).isEmpty();
  }

  @Test
  public void testPipelineOverridesAreIsolated() {
    LogicalTableUrlParser parser = new LogicalTableUrlParser(
        "jdbc:logical://nearline=xinfra-tracking;online=venice;pipeline.parallelism=4;pipeline.timeout=30s");

    Map<String, String> tiers = parser.tiers();
    assertThat(tiers).hasSize(2);
    assertThat(tiers).containsEntry("nearline", "xinfra-tracking");
    assertThat(tiers).containsEntry("online", "venice");

    Map<String, String> overrides = parser.pipelineOverrides();
    assertThat(overrides).hasSize(2);
    assertThat(overrides).containsEntry("parallelism", "4");
    assertThat(overrides).containsEntry("timeout", "30s");
  }

  @Test
  public void testEmptyUrl() {
    LogicalTableUrlParser parser = new LogicalTableUrlParser("jdbc:logical://");
    assertThat(parser.tiers()).isEmpty();
    assertThat(parser.pipelineOverrides()).isEmpty();
  }

  @Test
  public void testInvalidUrl() {
    assertThatThrownBy(() -> new LogicalTableUrlParser("jdbc:other://foo=bar"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("jdbc:logical://");
  }

  @Test
  public void testNullUrl() {
    assertThatThrownBy(() -> new LogicalTableUrlParser(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testTierOrderPreserved() {
    LogicalTableUrlParser parser = new LogicalTableUrlParser(
        "jdbc:logical://nearline=xinfra-tracking;offline=openhouse;online=venice");

    // LinkedHashMap preserves insertion order
    assertThat(parser.tiers().keySet()).containsExactly("nearline", "offline", "online");
  }
}
