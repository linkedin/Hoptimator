package com.linkedin.hoptimator.logical;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Parses a {@code jdbc:logical://} URL into a tier map and pipeline overrides.
 *
 * <p>URL format:
 * <pre>jdbc:logical://nearline=xinfra-tracking;offline=openhouse;online=venice</pre>
 *
 * <p>Each {@code {key}={value}} segment is split on the first {@code =}. Keys starting
 * with {@code pipeline.} are separated into the pipeline-overrides map (reserved for
 * future per-table pipeline configuration). All other keys are treated as tier names
 * mapping to Database CRD names.
 */
public final class LogicalTableUrlParser {

  static final String URL_PREFIX = "jdbc:logical://";
  static final String PIPELINE_PREFIX = "pipeline.";

  private final Map<String, String> tiers;
  private final Map<String, String> pipelineOverrides;

  /**
   * Parses the given JDBC URL.
   *
   * @param url the full JDBC URL, e.g. {@code jdbc:logical://nearline=xinfra-tracking;online=venice}
   * @throws IllegalArgumentException if the URL does not start with {@code jdbc:logical://}
   */
  public LogicalTableUrlParser(String url) {
    if (url == null || !url.startsWith(URL_PREFIX)) {
      throw new IllegalArgumentException("URL must start with " + URL_PREFIX + " but was: " + url);
    }
    String params = url.substring(URL_PREFIX.length());

    Map<String, String> parsedTiers = new LinkedHashMap<>();
    Map<String, String> parsedOverrides = new LinkedHashMap<>();

    if (!params.isEmpty()) {
      for (String segment : params.split(";")) {
        segment = segment.trim();
        if (segment.isEmpty()) {
          continue;
        }
        int eq = segment.indexOf('=');
        if (eq < 0) {
          // Bare key with no value — skip silently; could be a flag but we don't need it
          continue;
        }
        String key = segment.substring(0, eq).trim();
        String value = segment.substring(eq + 1).trim();
        if (key.startsWith(PIPELINE_PREFIX)) {
          parsedOverrides.put(key.substring(PIPELINE_PREFIX.length()), value);
        } else {
          parsedTiers.put(key, value);
        }
      }
    }

    this.tiers = Collections.unmodifiableMap(parsedTiers);
    this.pipelineOverrides = Collections.unmodifiableMap(parsedOverrides);
  }

  /**
   * Returns a map of tier name to Database CRD name.
   * E.g. {@code {nearline: xinfra-tracking, online: venice}}.
   */
  public Map<String, String> tiers() {
    return tiers;
  }

  /**
   * Returns pipeline override parameters — keys with the {@code pipeline.} prefix, stripped.
   * Reserved for future per-table pipeline configuration. Currently always empty.
   */
  public Map<String, String> pipelineOverrides() {
    return pipelineOverrides;
  }
}
