package com.linkedin.hoptimator.k8s;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;


/**
 * Computes the labels and annotations that encode a Pipeline CRD's dependency edges.
 *
 * <p>Every source and sink a pipeline references is recorded as a label:
 * {@code hoptimator.linkedin.com/depends-on-<slug>: "<database>_<pathString>"} where
 * {@code <slug>} is a deterministic hash derived from {@code database + "_" + pathString}.
 * The hash keeps label keys within Kubernetes's 63-character name limit for arbitrary paths,
 * and lets {@code K8sApi.select} filter pipelines by dependency on the server.
 *
 * <p>Two annotations preserve the directional information the labels lose:
 * <ul>
 *   <li>{@link #ANNOTATION_KEY_SOURCES} — comma-separated list of source identifiers verbatim.</li>
 *   <li>{@link #ANNOTATION_KEY_SINK} — the single sink identifier verbatim.</li>
 * </ul>
 * Together they serve two purposes:
 * <ol>
 *   <li><b>Collision guard</b> for the delete-time dependency checker — a label match is only
 *       trusted when the resource appears in either annotation, so slug collisions and stale
 *       labels (from {@link K8sApi#update}'s additive label merge) can't produce false positives.</li>
 *   <li><b>Direction recovery</b> for visualization — the renderer draws source → pipeline → sink
 *       arrows from the split.</li>
 * </ol>
 */
public final class PipelineDependencyLabels {

  static final String LABEL_PREFIX = "hoptimator.linkedin.com/depends-on-";
  /** Annotation listing only source identifiers. */
  public static final String ANNOTATION_KEY_SOURCES = "hoptimator.linkedin.com/depends-on-sources";
  /** Annotation holding the single sink identifier. */
  public static final String ANNOTATION_KEY_SINK = "hoptimator.linkedin.com/depends-on-sink";

  private static final int SLUG_LENGTH = 16;   // 64 bits of SHA-256 → ~1 in 1.8e19 collisions
  private static final int MAX_LABEL_VALUE = 63;

  private PipelineDependencyLabels() {
  }

  /**
   * Canonical logical identifier for a resource: {@code <database>_<dot-joined-path>}.
   */
  public static String identifier(String database, Iterable<String> path) {
    return database + "_" + String.join(".", path);
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
   * Labels to stamp on a Pipeline CRD — one entry per source <em>and</em> the sink. Both edges
   * matter to the guard: dropping a source orphans pipelines that read from it; dropping a sink
   * orphans pipelines that write to it.
   *
   * <p>Keys are the same as {@link #labelKey}. Values are the readable identifier, truncated
   * to 63 chars if necessary (the annotation preserves the untruncated form). Values are
   * for debugging purposes only.
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

  /** Comma-separated list of source identifiers (no sink). Empty string if no sources. */
  public static String sourcesAnnotation(Collection<Source> sources) {
    Set<String> ids = new LinkedHashSet<>();
    for (Source src : sources) {
      ids.add(identifier(src.database(), src.path()));
    }
    return String.join(",", ids);
  }

  /** Single sink identifier, or {@code null} if there is no sink. */
  public static String sinkAnnotation(Sink sink) {
    if (sink == null) {
      return null;
    }
    return identifier(sink.database(), sink.path());
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
