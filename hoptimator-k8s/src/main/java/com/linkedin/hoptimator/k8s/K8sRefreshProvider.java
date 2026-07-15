package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.RefreshProvider;
import com.linkedin.hoptimator.RefreshTarget;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * Resolves a {@code REFRESH} target against Kubernetes.
 *
 * <p>The object kind is discovered from the backing CRD: a {@code LogicalTable} makes it a
 * {@link RefreshTarget.Kind#TABLE}; a materialized {@code View} makes it a
 * {@link RefreshTarget.Kind#MATERIALIZED_VIEW}. Anything else (a plain view, a physical table, an
 * unknown name) is not refreshable, so {@link #resolve} returns {@code null}.
 *
 * <p>The triggers "immediately upstream" of the object are the {@code TableTrigger}s owned by the
 * object's CRD — the same owner reference the deployers stamp when they auto-create a trigger (e.g.
 * a LogicalTable's offline-tier backfill trigger). Firing them backfills the object.
 */
public class K8sRefreshProvider implements RefreshProvider {

  @Override
  public RefreshTarget resolve(List<String> path, Connection connection) throws SQLException {
    K8sContext context = K8sContext.create(connection);
    String name = K8sUtils.canonicalizeName(path);

    K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi =
        new K8sApi<>(context, K8sApiEndpoints.LOGICAL_TABLES);
    K8sApi<V1alpha1View, V1alpha1ViewList> viewApi = new K8sApi<>(context, K8sApiEndpoints.VIEWS);
    K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi =
        new K8sApi<>(context, K8sApiEndpoints.TABLE_TRIGGERS);

    return resolve(name, logicalTableApi, viewApi, triggerApi);
  }

  /** Package-private core, wired with injectable APIs for testing. */
  RefreshTarget resolve(String name,
      K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi,
      K8sApi<V1alpha1View, V1alpha1ViewList> viewApi,
      K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi) throws SQLException {

    V1alpha1LogicalTable logicalTable = logicalTableApi.getIfExists(name);
    if (logicalTable != null) {
      return new RefreshTarget(RefreshTarget.Kind.TABLE,
          triggersOwnedBy(triggerApi, K8sApiEndpoints.LOGICAL_TABLES.kind(), name));
    }

    V1alpha1View view = viewApi.getIfExists(name);
    if (view != null && view.getSpec() != null && Boolean.TRUE.equals(view.getSpec().getMaterialized())) {
      return new RefreshTarget(RefreshTarget.Kind.MATERIALIZED_VIEW,
          triggersOwnedBy(triggerApi, K8sApiEndpoints.VIEWS.kind(), name));
    }

    // Not a materialized view or logical table — this provider doesn't manage it.
    return null;
  }

  /** Names of the TableTriggers whose owner reference points at {@code (ownerKind, ownerName)}. */
  private static List<String> triggersOwnedBy(
      K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi,
      String ownerKind, String ownerName) throws SQLException {
    List<String> names = new ArrayList<>();
    for (V1alpha1TableTrigger trigger : triggerApi.list()) {
      V1ObjectMeta meta = trigger.getMetadata();
      if (meta == null || meta.getName() == null || meta.getOwnerReferences() == null) {
        continue;
      }
      for (V1OwnerReference owner : meta.getOwnerReferences()) {
        if (ownerKind.equals(owner.getKind()) && ownerName.equals(owner.getName())) {
          names.add(meta.getName());
          break;
        }
      }
    }
    return names;
  }
}
