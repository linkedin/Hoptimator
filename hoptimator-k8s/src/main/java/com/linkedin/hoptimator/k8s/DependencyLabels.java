package com.linkedin.hoptimator.k8s;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;


/**
 * Encodes a resource's dependency edges as K8s labels + annotations. Used by both
 * {@link K8sPipelineDeployer} and {@link K8sTriggerDeployer} so the dep-guard can
 * find dependents with a single label-selector query.
 *
 * <p>Every source and sink the resource references becomes a label:
 * {@code hoptimator.linkedin.com/depends-on-<slug>: "<database>_<pathString>"} where
 * {@code <slug>} is a deterministic hash derived from {@code database + "_" + pathString}.
 * The hash keeps label keys within Kubernetes's 63-character name limit for arbitrary paths,
 * and lets {@code K8sApi.select} filter by dependency on the server.
 *
 * <p>Two annotations preserve the directional information the labels lose:
 * <ul>
 *   <li>{@link #ANNOTATION_KEY_SOURCES} — comma-separated list of source identifiers verbatim.</li>
 *   <li>{@link #ANNOTATION_KEY_SINKS} — comma-separated list of sink identifiers verbatim.</li>
 * </ul>
 * Together they serve two purposes:
 * <ol>
 *   <li><b>Collision guard</b> for the delete-time dependency checker — a label match is only
 *       trusted when the resource appears in either annotation, so slug collisions and stale
 *       labels (from {@link K8sApi#update}'s additive label merge) can't produce false positives.</li>
 *   <li><b>Direction recovery</b> for visualization — the renderer draws source → resource → sink
 *       arrows from the split.</li>
 * </ol>
 */
public final class DependencyLabels {

  static final String LABEL_PREFIX = "hoptimator.linkedin.com/depends-on-";
  /** Annotation listing source identifiers, comma-separated. */
  public static final String ANNOTATION_KEY_SOURCES = "hoptimator.linkedin.com/depends-on-sources";
  /** Annotation listing sink identifiers, comma-separated. */
  public static final String ANNOTATION_KEY_SINKS = "hoptimator.linkedin.com/depends-on-sinks";

  private static final int SLUG_LENGTH = 16;   // 64 bits of SHA-256 → ~1 in 1.8e19 collisions
  private static final int MAX_LABEL_VALUE = 63;

  private DependencyLabels() {
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

  /** Label key a resource carries if it depends on the given source/sink. */
  public static String labelKey(String database, Iterable<String> path) {
    return LABEL_PREFIX + slug(database, path);
  }

  /**
   * Stamps the depends-on labels and directional annotations onto {@code meta}, merging with any
   * existing labels/annotations on the object. Both edges matter to the guard: dropping a source
   * orphans resources that read from it; dropping a sink orphans resources that write to it.
   *
   * <p>Sources and sinks whose {@code database()} is null are skipped — they don't have a stable
   * identifier. Callers with a single sink should pass {@link Collections#singletonList}.
   */
  public static void stamp(V1ObjectMeta meta, Collection<Source> sources, Collection<Sink> sinks) {
    Map<String, String> labels = meta.getLabels() != null ? meta.getLabels() : new HashMap<>();
    Map<String, String> annotations = meta.getAnnotations() != null ? meta.getAnnotations() : new HashMap<>();
    Set<String> sourceIds = collectIdentifiers(sources, labels);
    Set<String> sinkIds = collectIdentifiers(sinks, labels);
    if (!sourceIds.isEmpty()) {
      annotations.put(ANNOTATION_KEY_SOURCES, String.join(",", sourceIds));
    }
    if (!sinkIds.isEmpty()) {
      annotations.put(ANNOTATION_KEY_SINKS, String.join(",", sinkIds));
    }
    meta.setLabels(labels);
    meta.setAnnotations(annotations);
  }

  private static Set<String> collectIdentifiers(Collection<? extends Source> deps,
      Map<String, String> labels) {
    Set<String> ids = new LinkedHashSet<>();
    if (deps == null) {
      return ids;
    }
    for (Source dep : deps) {
      if (dep == null || dep.database() == null) {
        continue;
      }
      String id = identifier(dep.database(), dep.path());
      labels.put(labelKey(dep.database(), dep.path()), truncate(id));
      ids.add(id);
    }
    return ids;
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
