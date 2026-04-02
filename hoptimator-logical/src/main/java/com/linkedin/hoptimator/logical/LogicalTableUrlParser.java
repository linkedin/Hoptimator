package com.linkedin.hoptimator.logical;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Parses a {@code jdbc:logical://} URL into tier and reserved parameter maps.
 *
 * <p>URL format:
 * <pre>jdbc:logical://nearline=xinfra-tracking;offline=openhouse;online=venice</pre>
 *
 * <p>Each {@code {tier-name}={database-crd-name}} declares a tier. Parameters
 * prefixed with {@code pipeline.} are separated into a reserved map for future
 * per-table pipeline override support.
 */
public final class LogicalTableUrlParser {

  public static final String CONNECT_STRING_PREFIX = "jdbc:logical://";
  static final String PIPELINE_PREFIX = "pipeline.";

  private final String url;

  /** Instance-based API: parse once, query multiple times. */
  public LogicalTableUrlParser(String url) {
    if (url == null || !url.startsWith(CONNECT_STRING_PREFIX)) {
      throw new IllegalArgumentException(
          "URL must start with " + CONNECT_STRING_PREFIX + " but was: " + url);
    }
    this.url = url;
  }

  /** Returns tier name → Database CRD name mappings. */
  public Map<String, String> tiers() {
    return tierMap(url);
  }

  /** Returns reserved {@code pipeline.*} overrides (without the prefix). */
  public Map<String, String> pipelineOverrides() {
    return pipelineParams(url);
  }

  /**
   * Parses the URL and returns tier name to Database CRD name mappings.
   * {@code pipeline.*} keys are excluded from the returned map.
   *
   * @param url full JDBC URL, e.g. {@code jdbc:logical://nearline=foo;offline=bar}
   * @return unmodifiable map of tier-name → database-crd-name
   */
  public static Map<String, String> tierMap(String url) {
    return parse(url).tierMap;
  }

  /**
   * Parses the URL and returns reserved {@code pipeline.*} parameters
   * (without the {@code pipeline.} prefix).
   *
   * @param url full JDBC URL
   * @return unmodifiable map of stripped pipeline param keys → values
   */
  public static Map<String, String> pipelineParams(String url) {
    return parse(url).pipelineParams;
  }

  /** Parsed result holding both maps. */
  static ParseResult parse(String url) {
    String suffix = url.startsWith(CONNECT_STRING_PREFIX)
        ? url.substring(CONNECT_STRING_PREFIX.length())
        : url;

    Map<String, String> tiers = new LinkedHashMap<>();
    Map<String, String> pipeline = new LinkedHashMap<>();

    if (suffix.isEmpty()) {
      return new ParseResult(Collections.unmodifiableMap(tiers), Collections.unmodifiableMap(pipeline));
    }

    for (String segment : suffix.split(";")) {
      int eq = segment.indexOf('=');
      if (eq < 0) {
        continue; // skip malformed segment
      }
      String key = segment.substring(0, eq).trim();
      String value = segment.substring(eq + 1).trim();
      if (key.startsWith(PIPELINE_PREFIX)) {
        pipeline.put(key.substring(PIPELINE_PREFIX.length()), value);
      } else {
        tiers.put(key, value);
      }
    }

    return new ParseResult(Collections.unmodifiableMap(tiers), Collections.unmodifiableMap(pipeline));
  }

  static final class ParseResult {
    final Map<String, String> tierMap;
    final Map<String, String> pipelineParams;

    ParseResult(Map<String, String> tierMap, Map<String, String> pipelineParams) {
      this.tierMap = tierMap;
      this.pipelineParams = pipelineParams;
    }
  }
}
