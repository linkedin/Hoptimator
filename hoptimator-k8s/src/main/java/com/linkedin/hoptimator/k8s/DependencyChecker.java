package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;

import javax.annotation.Nullable;


/**
 * Checks whether any Pipeline or TableTrigger CRDs still depend on a resource a
 * {@link com.linkedin.hoptimator.Deployer} is about to delete.
 *
 * <p>Both CRDs carry the same {@code depends-on-<slug>} label and {@code depends-on-sources}/
 * {@code depends-on-sink} annotations (stamped by {@link K8sPipelineDeployer} and
 * {@link K8sTriggerDeployer}), so the same lookup works for either: a label-selector list against
 * the CRD group is O(matches) on the wire, then each candidate is cross-checked against the union
 * of the source + sink annotations to rule out hash collisions in the slug and stale labels left
 * over from a prior version of the resource ({@link K8sApi#update}'s additive label merge can leak
 * old {@code depends-on-*} keys).
 *
 * <p>Resources owned (directly) by {@code (selfOwnerKind, selfOwnerName)} are excluded from the
 * blocker list: those will be cascade-deleted alongside the parent resource, so counting them as
 * external dependents would make composite deletes (e.g. {@code LogicalTableDeployer.delete()})
 * impossible.
 */
public final class DependencyChecker {

  private DependencyChecker() {
  }

  public static void assertNoExternalDependents(K8sContext context, String database,
      List<String> path, @Nullable String selfOwnerKind, @Nullable String selfOwnerName) throws SQLException {
    assertNoExternalDependents(
        new K8sApi<>(context, K8sApiEndpoints.PIPELINES),
        new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS),
        database, path, selfOwnerKind, selfOwnerName);
  }

  /** Variant that takes pre-built {@link K8sApi}s — used by tests to inject mocks. */
  static void assertNoExternalDependents(K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi,
      K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi,
      String database, List<String> path, @Nullable String selfOwnerKind,
      @Nullable String selfOwnerName) throws SQLException {

    String labelKey = DependencyLabels.labelKey(database, path);
    String identifier = DependencyLabels.identifier(database, path);

    List<String> blockers = new ArrayList<>();
    blockers.addAll(findBlockers(pipelineApi, labelKey, identifier, "pipeline",
        selfOwnerKind, selfOwnerName));
    blockers.addAll(findBlockers(triggerApi, labelKey, identifier, "trigger",
        selfOwnerKind, selfOwnerName));

    if (!blockers.isEmpty()) {
      throw new SQLException(String.format(
          "Cannot delete %s — %d active dependent(s): %s",
          identifier, blockers.size(), String.join(", ", blockers)));
    }
  }

  /**
   * Generic blocker enumeration: list resources of type {@code T} that carry the given
   * {@code labelKey}, confirm via the depends-on annotations, and exclude self-owned children.
   * The {@code kindLabel} is prefixed onto each blocker description so a unified error message
   * can attribute each entry to its CRD kind.
   */
  private static <T extends KubernetesObject> List<String> findBlockers(K8sApi<T, ?> api,
      String labelKey, String identifier, String kindLabel,
      @Nullable String selfOwnerKind, @Nullable String selfOwnerName) throws SQLException {
    Collection<T> matches = api.select(labelKey);
    List<String> blockers = new ArrayList<>();
    for (T obj : matches) {
      V1ObjectMeta meta = obj.getMetadata();
      if (isSelfOwned(meta, selfOwnerKind, selfOwnerName)) {
        continue;
      }
      if (!annotationConfirms(meta, identifier)) {
        // Label matched but annotation doesn't — slug collision or stale label, skip it.
        continue;
      }
      blockers.add(kindLabel + "/" + describeBlocker(meta));
    }
    return blockers;
  }

  private static boolean isSelfOwned(V1ObjectMeta meta, @Nullable String selfOwnerKind,
      @Nullable String selfOwnerName) {
    if (selfOwnerKind == null || selfOwnerName == null) {
      return false;
    }
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

  private static boolean annotationConfirms(V1ObjectMeta meta, String identifier) {
    if (meta == null || meta.getAnnotations() == null) {
      return true;   // pre-labeling resource — conservatively trust the label match
    }
    String sourcesAnno = meta.getAnnotations().get(DependencyLabels.ANNOTATION_KEY_SOURCES);
    String sinkAnno = meta.getAnnotations().get(DependencyLabels.ANNOTATION_KEY_SINK);
    if (sourcesAnno == null && sinkAnno == null) {
      return true;   // same — no annotations to cross-check against
    }
    if (sourcesAnno != null && DependencyLabels.parseAnnotation(sourcesAnno).contains(identifier)) {
      return true;
    }
    return identifier.equals(sinkAnno);
  }

  /**
   * Builds a human-readable blocker description: the resource name, plus (when present) the top
   * ownerReference's {@code kind/name} so the user knows which higher-level resource owns it.
   */
  private static String describeBlocker(V1ObjectMeta meta) {
    String name = meta == null ? "<unknown>" : meta.getName();
    String ownerSuffix = "";
    if (meta != null && meta.getOwnerReferences() != null && !meta.getOwnerReferences().isEmpty()) {
      V1OwnerReference owner = meta.getOwnerReferences().get(0);
      ownerSuffix = " (owned by " + owner.getKind() + "/" + owner.getName() + ")";
    }
    return name + ownerSuffix;
  }
}
