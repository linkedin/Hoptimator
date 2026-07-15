package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.RefreshTarget;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class K8sRefreshProviderTest {

  private static final String NAME = "logical-offline-members";

  @Mock
  private K8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> logicalTableApi;
  @Mock
  private K8sApi<V1alpha1View, V1alpha1ViewList> viewApi;
  @Mock
  private K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApi;

  private final K8sRefreshProvider provider = new K8sRefreshProvider();

  private static V1alpha1TableTrigger trigger(String name, String ownerKind, String ownerName) {
    V1ObjectMeta meta = new V1ObjectMeta().name(name);
    if (ownerKind != null) {
      meta.addOwnerReferencesItem(new V1OwnerReference().kind(ownerKind).name(ownerName));
    }
    return new V1alpha1TableTrigger().metadata(meta);
  }

  @Test
  void resolvesLogicalTableAndItsOwnedTriggers() throws SQLException {
    when(logicalTableApi.getIfExists(NAME)).thenReturn(new V1alpha1LogicalTable());
    when(triggerApi.list()).thenReturn(Arrays.asList(
        trigger("logical-members-offline-trigger", "LogicalTable", NAME),
        trigger("some-other-trigger", "LogicalTable", "other-table"),
        trigger("orphan-trigger", null, null)));

    RefreshTarget target = provider.resolve(NAME, logicalTableApi, viewApi, triggerApi);

    assertThat(target).isNotNull();
    assertThat(target.kind()).isEqualTo(RefreshTarget.Kind.TABLE);
    assertThat(target.triggerNames()).containsExactly("logical-members-offline-trigger");
  }

  @Test
  void resolvesMaterializedView() throws SQLException {
    when(logicalTableApi.getIfExists(NAME)).thenReturn(null);
    when(viewApi.getIfExists(NAME)).thenReturn(
        new V1alpha1View().spec(new V1alpha1ViewSpec().materialized(true)));
    when(triggerApi.list()).thenReturn(Collections.singletonList(
        trigger("mv-trigger", "View", NAME)));

    RefreshTarget target = provider.resolve(NAME, logicalTableApi, viewApi, triggerApi);

    assertThat(target).isNotNull();
    assertThat(target.kind()).isEqualTo(RefreshTarget.Kind.MATERIALIZED_VIEW);
    assertThat(target.triggerNames()).containsExactly("mv-trigger");
  }

  @Test
  void logicalTableWithNoOwnedTriggersResolvesToEmpty() throws SQLException {
    when(logicalTableApi.getIfExists(NAME)).thenReturn(new V1alpha1LogicalTable());
    when(triggerApi.list()).thenReturn(Collections.singletonList(
        trigger("unrelated", "LogicalTable", "other")));

    RefreshTarget target = provider.resolve(NAME, logicalTableApi, viewApi, triggerApi);

    assertThat(target).isNotNull();
    assertThat(target.kind()).isEqualTo(RefreshTarget.Kind.TABLE);
    assertThat(target.triggerNames()).isEmpty();
  }

  @Test
  void plainViewIsNotRefreshable() throws SQLException {
    when(logicalTableApi.getIfExists(NAME)).thenReturn(null);
    when(viewApi.getIfExists(NAME)).thenReturn(
        new V1alpha1View().spec(new V1alpha1ViewSpec().materialized(false)));

    RefreshTarget target = provider.resolve(NAME, logicalTableApi, viewApi, triggerApi);

    assertThat(target).isNull();
  }

  @Test
  void unknownObjectIsNotRefreshable() throws SQLException {
    when(logicalTableApi.getIfExists(NAME)).thenReturn(null);
    when(viewApi.getIfExists(NAME)).thenReturn(null);

    RefreshTarget target = provider.resolve(NAME, logicalTableApi, viewApi, triggerApi);

    assertThat(target).isNull();
  }
}
