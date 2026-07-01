package com.linkedin.hoptimator.operator.trigger;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.FakeK8sYamlApi;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Verifies the source-agnostic data-availability path: a trigger whose input is covered by an
 * {@link InputWatermarkProvider} advances its cursor to the provider's data-time frontier, with no
 * source-specific logic in the reconciler.
 */
class TableTriggerReconcilerWatermarkTest {

  private static final String JOB_TEMPLATE = String.join("\n",
      "apiVersion: batch/v1",
      "kind: Job",
      "metadata:",
      "  name: wm-job");

  private final List<V1Job> jobs = new ArrayList<>();
  private final List<V1alpha1TableTrigger> triggers = new ArrayList<>();
  private final Map<String, String> yamls = new HashMap<>();

  @BeforeEach
  void beforeEach() {
    jobs.clear();
    triggers.clear();
    yamls.clear();
  }

  private TableTriggerReconciler reconcilerWithFrontier(Instant frontier) {
    InputWatermarkProvider provider = input -> Optional.ofNullable(frontier);
    return new TableTriggerReconciler(new FakeK8sApi<>(triggers), new FakeK8sApi<>(jobs),
        new FakeK8sYamlApi(yamls), Collections.singletonList(provider));
  }

  private V1alpha1TableTrigger trigger(V1alpha1TableTriggerSpec spec, V1alpha1TableTriggerStatus status) {
    V1alpha1TableTrigger t = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("wm-trigger").namespace("namespace"))
        .spec(spec.yaml(JOB_TEMPLATE).schema("KAFKA").table("events"));
    if (status != null) {
      t.status(status);
    }
    triggers.add(t);
    return t;
  }

  @Test
  void advancesCursorToFrontier() {
    Instant frontier = OffsetDateTime.of(2026, 5, 8, 10, 0, 0, 0, ZoneOffset.UTC).toInstant();
    trigger(new V1alpha1TableTriggerSpec(), null);

    reconcilerWithFrontier(frontier).reconcile(new Request("namespace", "wm-trigger"));

    OffsetDateTime timestamp = triggers.get(0).getStatus().getTimestamp();
    assertThat(timestamp).isEqualTo(OffsetDateTime.of(2026, 5, 8, 10, 0, 0, 0, ZoneOffset.UTC));
  }

  @Test
  void doesNotAdvanceWhenFrontierNotBeyondCursor() {
    // Cursor already at the frontier: nothing new is complete, so the cursor stays put.
    OffsetDateTime cursor = OffsetDateTime.of(2026, 5, 8, 10, 0, 0, 0, ZoneOffset.UTC);
    Instant frontier = cursor.toInstant();
    trigger(new V1alpha1TableTriggerSpec(),
        new V1alpha1TableTriggerStatus().timestamp(cursor).watermark(cursor));

    reconcilerWithFrontier(frontier).reconcile(new Request("namespace", "wm-trigger"));

    assertThat(triggers.get(0).getStatus().getTimestamp()).isEqualTo(cursor);
  }

  @Test
  void skipsWhenNoProviderHandlesInputAndNoScheduleOrStatus() {
    // Provider returns empty (does not handle this input); with no schedule and no status, the
    // trigger is inert — uniform fallback to cron/manual firing.
    trigger(new V1alpha1TableTriggerSpec(), null);
    InputWatermarkProvider empty = input -> Optional.empty();
    new TableTriggerReconciler(new FakeK8sApi<>(triggers), new FakeK8sApi<>(jobs),
        new FakeK8sYamlApi(yamls), Collections.singletonList(empty))
        .reconcile(new Request("namespace", "wm-trigger"));

    assertThat(triggers.get(0).getStatus()).isNull();
  }

  // ---- late-change repair (out-of-order writes behind the watermark -> one-off backfill) ----

  private TableTriggerReconciler reconcilerWith(Instant frontier, List<DataChange> changes) {
    InputWatermarkProvider provider = new InputWatermarkProvider() {
      @Override
      public Optional<Instant> completeThrough(TriggerInput input) {
        return Optional.ofNullable(frontier);
      }

      @Override
      public List<DataChange> changesSince(TriggerInput input, Instant sinceArrival) {
        return changes;
      }
    };
    return new TableTriggerReconciler(new FakeK8sApi<>(triggers), new FakeK8sApi<>(jobs),
        new FakeK8sYamlApi(yamls), Collections.singletonList(provider));
  }

  private static OffsetDateTime odt(int hour, int minute) {
    return OffsetDateTime.of(2026, 5, 8, hour, minute, 0, 0, ZoneOffset.UTC);
  }

  private static Instant inst(int hour, int minute) {
    return odt(hour, minute).toInstant();
  }

  @Test
  void repairsLateChangeBehindWatermarkViaBackfill() {
    OffsetDateTime watermark = odt(12, 0);
    trigger(new V1alpha1TableTriggerSpec(), new V1alpha1TableTriggerStatus()
        .timestamp(watermark).watermark(watermark).lateWatermark(odt(8, 0)));
    DataChange late = new DataChange(inst(9, 30), inst(9, 0), inst(10, 0));

    reconcilerWith(null, Collections.singletonList(late)).reconcile(new Request("namespace", "wm-trigger"));

    V1alpha1TableTriggerStatus s = triggers.get(0).getStatus();
    assertThat(s.getBackfillFrom()).isEqualTo(odt(9, 0));
    assertThat(s.getBackfillTo()).isEqualTo(odt(10, 0));
    assertThat(s.getLateWatermark()).isEqualTo(odt(9, 30));
    // The forward cursor is untouched.
    assertThat(s.getWatermark()).isEqualTo(watermark);
    assertThat(s.getTimestamp()).isEqualTo(watermark);
  }

  @Test
  void clipsLateRepairWindowToWatermark() {
    OffsetDateTime watermark = odt(12, 0);
    trigger(new V1alpha1TableTriggerSpec(), new V1alpha1TableTriggerStatus()
        .timestamp(watermark).watermark(watermark).lateWatermark(odt(8, 0)));
    // Window straddles the watermark; the backfill end is clipped to the watermark.
    DataChange straddling = new DataChange(inst(13, 0), inst(11, 0), inst(13, 0));

    reconcilerWith(null, Collections.singletonList(straddling)).reconcile(new Request("namespace", "wm-trigger"));

    V1alpha1TableTriggerStatus s = triggers.get(0).getStatus();
    assertThat(s.getBackfillFrom()).isEqualTo(odt(11, 0));
    assertThat(s.getBackfillTo()).isEqualTo(watermark);
  }

  @Test
  void consumesAheadOfWatermarkChangeWithoutBackfill() {
    OffsetDateTime watermark = odt(12, 0);
    trigger(new V1alpha1TableTriggerSpec(), new V1alpha1TableTriggerStatus()
        .timestamp(watermark).watermark(watermark).lateWatermark(odt(8, 0)));
    // Change is ahead of the cursor; forward processing handles it, no repair, cursor consumed.
    DataChange ahead = new DataChange(inst(13, 30), inst(13, 0), inst(14, 0));

    reconcilerWith(null, Collections.singletonList(ahead)).reconcile(new Request("namespace", "wm-trigger"));

    V1alpha1TableTriggerStatus s = triggers.get(0).getStatus();
    assertThat(s.getBackfillFrom()).isNull();
    assertThat(s.getLateWatermark()).isEqualTo(odt(13, 30));
  }

  @Test
  void initializesLateWatermarkOnFirstSightWithoutBackfill() {
    OffsetDateTime watermark = odt(12, 0);
    trigger(new V1alpha1TableTriggerSpec(), new V1alpha1TableTriggerStatus()
        .timestamp(watermark).watermark(watermark));
    DataChange late = new DataChange(inst(9, 30), inst(9, 0), inst(10, 0));

    reconcilerWith(null, Collections.singletonList(late)).reconcile(new Request("namespace", "wm-trigger"));

    V1alpha1TableTriggerStatus s = triggers.get(0).getStatus();
    assertThat(s.getLateWatermark()).isNotNull();
    assertThat(s.getBackfillFrom()).isNull();
  }
}
