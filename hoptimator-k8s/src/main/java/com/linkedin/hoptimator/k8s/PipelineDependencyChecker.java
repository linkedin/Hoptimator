package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;

import javax.annotation.Nullable;


/**
 * Checks whether any Pipeline CRDs still depend on a resource a {@link com.linkedin.hoptimator.Deployer}
 * is about to delete.
 *
 * <p>The lookup is a label-selector list against the Pipeline CRD group, so it is O(matches) on
 * the wire — not a full scan. Each candidate is then cross-checked against the union of the
 * {@link PipelineDependencyLabels#ANNOTATION_KEY_SOURCES sources} and
 * {@link PipelineDependencyLabels#ANNOTATION_KEY_SINK sink} annotations to rule out hash
 * collisions in the label slug and stale labels left over from a prior version of the pipeline's
 * SQL ({@link K8sApi#update}'s additive label merge can leak old {@code depends-on-*} keys).
 *
 * <p>Pipelines owned (directly) by {@code (selfOwnerKind, selfOwnerName)} are excluded from the
 * blocker list: those pipelines will be cascade-deleted alongside the parent resource, so counting
 * them as external dependents would make composite deletes (e.g. {@code LogicalTableDeployer.delete()})
 * impossible.
 */
public final class PipelineDependencyChecker {

  private PipelineDependencyChecker() {
  }

  public static void assertNoExternalDependents(K8sContext context, String database,
      List<String> path, @Nullable String selfOwnerKind, @Nullable String selfOwnerName) throws SQLException {
    assertNoExternalDependents(new K8sApi<>(context, K8sApiEndpoints.PIPELINES),
        database, path, selfOwnerKind, selfOwnerName);
  }

  /** Variant that takes a pre-built {@link K8sApi} — used by tests to inject mocks. */
  static void assertNoExternalDependents(K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> api,
      String database, List<String> path, @Nullable String selfOwnerKind,
      @Nullable String selfOwnerName) throws SQLException {

    String labelKey = PipelineDependencyLabels.labelKey(database, path);
    String identifier = PipelineDependencyLabels.identifier(database, path);

    Collection<V1alpha1Pipeline> matches = api.select(labelKey);

    List<String> blockers = new ArrayList<>();
    for (V1alpha1Pipeline p : matches) {
      if (isSelfOwned(p, selfOwnerKind, selfOwnerName)) {
        continue;
      }
      if (!annotationConfirms(p, identifier)) {
        // Label matched but annotation doesn't — this is a slug collision or a stale label, skip it.
        continue;
      }
      blockers.add(describeBlocker(p));
    }

    if (!blockers.isEmpty()) {
      throw new SQLException(String.format(
          "Cannot delete %s — %d active pipeline(s) depend on it: %s",
          identifier, blockers.size(), String.join(", ", blockers)));
    }
  }

  private static boolean isSelfOwned(V1alpha1Pipeline pipeline, @Nullable String selfOwnerKind,
      @Nullable String selfOwnerName) {
    if (selfOwnerKind == null || selfOwnerName == null) {
      return false;
    }
    V1ObjectMeta meta = pipeline.getMetadata();
    if (meta == null || meta.getOwnerReferences() == null) {
      return false;
    }
    for (V1OwnerReference owner : meta.getOwnerReferences()) {
      if (selfOwnerKind.equals(owner.getKind()) && selfOwnerName.equals(owner.getName())) {
        return true;
      }
    }
    return false;
  }

  private static boolean annotationConfirms(V1alpha1Pipeline pipeline, String identifier) {
    V1ObjectMeta meta = pipeline.getMetadata();
    if (meta == null || meta.getAnnotations() == null) {
      return true;   // pre-labeling pipeline — conservatively trust the label match
    }
    String sourcesAnno = meta.getAnnotations().get(PipelineDependencyLabels.ANNOTATION_KEY_SOURCES);
    String sinkAnno = meta.getAnnotations().get(PipelineDependencyLabels.ANNOTATION_KEY_SINK);
    if (sourcesAnno == null && sinkAnno == null) {
      return true;   // same — no annotations to cross-check against
    }
    if (sourcesAnno != null && PipelineDependencyLabels.parseAnnotation(sourcesAnno).contains(identifier)) {
      return true;
    }
    return identifier.equals(sinkAnno);
  }

  /**
   * Builds a human-readable blocker description: the pipeline name, plus (when present) the top
   * ownerReference's {@code kind/name} so the user knows which higher-level resource owns it.
   */
  private static String describeBlocker(V1alpha1Pipeline pipeline) {
    V1ObjectMeta meta = pipeline.getMetadata();
    String name = meta == null ? "<unknown>" : meta.getName();
    String ownerSuffix = "";
    if (meta != null && meta.getOwnerReferences() != null && !meta.getOwnerReferences().isEmpty()) {
      V1OwnerReference owner = meta.getOwnerReferences().get(0);
      ownerSuffix = " (owned by " + owner.getKind() + "/" + owner.getName() + ")";
    }
    return name + ownerSuffix;
  }
}
