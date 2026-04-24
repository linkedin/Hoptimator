package com.linkedin.hoptimator.k8s;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;


/**
 * Computes the labels and annotation that encode a Pipeline CRD's dependency edges.
 *
 * <p>Every source and sink a pipeline references is recorded as a label:
 * {@code hoptimator.linkedin.com/depends-on-<slug>: "<database>/<pathString>"} where
 * {@code <slug>} is a deterministic hash derived from {@code database + "/" + pathString}.
 * The hash keeps label keys within Kubernetes's 63-character name limit for arbitrary paths,
 * and lets {@code K8sApi.select} filter pipelines by dependency on the server.
 *
 * <p>A collision-guard annotation ({@code ANNOTATION_KEY}) lists all logical identifiers verbatim,
 * so the delete-time check can distinguish a real dependency match from a rare hash collision.
 */
public final class PipelineDependencyLabels {

  static final String LABEL_PREFIX = "hoptimator.linkedin.com/depends-on-";
  public static final String ANNOTATION_KEY = "hoptimator.linkedin.com/depends-on";

  private static final int SLUG_LENGTH = 16;   // 64 bits of SHA-256 → ~1 in 1.8e19 collisions
  private static final int MAX_LABEL_VALUE = 63;

  private PipelineDependencyLabels() {
  }

  /** Canonical logical identifier for a resource: {@code <database>/<dot-joined-path>}. */
  public static String identifier(String database, Iterable<String> path) {
    return database + "/" + String.join(".", path);
  }

  /** Hex slug derived from the full identifier; same identifier always produces the same slug. */
  public static String slug(String database, Iterable<String> path) {
    byte[] digest = sha256(identifier(database, path).getBytes(StandardCharsets.UTF_8));
    StringBuilder sb = new StringBuilder(SLUG_LENGTH);
    for (int i = 0; i < SLUG_LENGTH / 2; i++) {
      sb.append(String.format("%02x", digest[i]));
    }
    return sb.toString();
  }

  /** Label key a Pipeline carries if it depends on the given resource. */
  public static String labelKey(String database, Iterable<String> path) {
    return LABEL_PREFIX + slug(database, path);
  }

  /**
   * Labels to stamp on a Pipeline CRD — one entry per source/sink. Keys are the same as
   * {@link #labelKey}. Values are the readable identifier, truncated to 63 chars if necessary
   * (the annotation preserves the untruncated form).
   */
  public static Map<String, String> labelsFor(Collection<Source> sources, Sink sink) {
    Map<String, String> labels = new LinkedHashMap<>();
    for (Source src : sources) {
      labels.put(labelKey(src.database(), src.path()), truncate(identifier(src.database(), src.path())));
    }
    if (sink != null) {
      labels.put(labelKey(sink.database(), sink.path()), truncate(identifier(sink.database(), sink.path())));
    }
    return labels;
  }

  /**
   * Collision-guard annotation value — comma-separated list of full identifiers, deduplicated.
   * The delete-time check cross-references this annotation after the label selector narrows the
   * candidate set.
   */
  public static String annotationFor(Collection<Source> sources, Sink sink) {
    Set<String> ids = new LinkedHashSet<>();
    for (Source src : sources) {
      ids.add(identifier(src.database(), src.path()));
    }
    if (sink != null) {
      ids.add(identifier(sink.database(), sink.path()));
    }
    return String.join(",", ids);
  }

  /**
   * Removes any {@code depends-on-*} entries from an existing label map so that an update can
   * restamp the current dependency set without carrying stale labels from an earlier version of
   * the pipeline. {@link K8sApi#update} is additive for labels; callers must strip first.
   */
  public static Map<String, String> stripDependencyLabels(Map<String, String> labels) {
    if (labels == null) {
      return new LinkedHashMap<>();
    }
    return labels.entrySet().stream()
        .filter(e -> !e.getKey().startsWith(LABEL_PREFIX))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
            (a, b) -> a, LinkedHashMap::new));
  }

  /** Parses the collision-guard annotation back into the set of identifiers it encoded. */
  public static Set<String> parseAnnotation(String annotation) {
    Set<String> out = new LinkedHashSet<>();
    if (annotation == null || annotation.isEmpty()) {
      return out;
    }
    for (String id : annotation.split(",")) {
      String trimmed = id.trim();
      if (!trimmed.isEmpty()) {
        out.add(trimmed);
      }
    }
    return out;
  }

  private static String truncate(String value) {
    return value.length() <= MAX_LABEL_VALUE ? value : value.substring(0, MAX_LABEL_VALUE);
  }

  private static byte[] sha256(byte[] input) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(input);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }
}
