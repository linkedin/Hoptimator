package com.linkedin.hoptimator.operator.trigger;

import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewStatus;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ViewReconcilerTest {

  @Mock
  private K8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> mockTriggerApi;

  @Mock
  private K8sApi<V1alpha1View, V1alpha1ViewList> mockViewApi;

  private List<V1alpha1TableTrigger> triggers;
  private List<V1alpha1View> views;
  private ViewReconciler reconciler;

  @BeforeEach
  void setUp() {
    triggers = new ArrayList<>();
    views = new ArrayList<>();
    reconciler = new ViewReconciler(
        new FakeK8sApi<>(triggers),
        new FakeK8sApi<>(views));
  }

  @Test
  void reconcileDeletedViewDoesNotRequeue() {
    Result result = reconciler.reconcile(new Request("ns", "deleted-view"));
    assertFalse(result.isRequeue());
  }

  @Test
  void reconcileViewWithNullStatusDoesNotRequeue() {
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("view-no-status"));
    views.add(view);

    Result result = reconciler.reconcile(new Request("ns", "view-no-status"));
    assertFalse(result.isRequeue());
  }

  @Test
  void reconcileViewWithNullWatermarkDoesNotRequeue() {
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("view-null-watermark"))
        .status(new V1alpha1ViewStatus());
    views.add(view);

    Result result = reconciler.reconcile(new Request("ns", "view-null-watermark"));
    assertFalse(result.isRequeue());
  }

  @Test
  void reconcileViewWithWatermarkFiresTriggers() {
    OffsetDateTime watermark = OffsetDateTime.now();
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("view-with-watermark"))
        .status(new V1alpha1ViewStatus().watermark(watermark));
    views.add(view);

    Map<String, String> labels = new HashMap<>();
    labels.put("view", "view-with-watermark");
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("trigger-1").labels(labels));
    triggers.add(trigger);

    Result result = reconciler.reconcile(new Request("ns", "view-with-watermark"));
    assertFalse(result.isRequeue());
    assertNotNull(trigger.getStatus());
    assertEquals(watermark, trigger.getStatus().getTimestamp());
  }

  @Test
  void reconcileViewUpdatesExistingTriggerStatus() {
    OffsetDateTime watermark = OffsetDateTime.now();
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("view-update"))
        .status(new V1alpha1ViewStatus().watermark(watermark));
    views.add(view);

    Map<String, String> labels = new HashMap<>();
    labels.put("view", "view-update");
    V1alpha1TableTriggerStatus existingStatus = new V1alpha1TableTriggerStatus()
        .timestamp(OffsetDateTime.now().minusHours(1));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("trigger-existing").labels(labels))
        .status(existingStatus);
    triggers.add(trigger);

    Result result = reconciler.reconcile(new Request("ns", "view-update"));
    assertFalse(result.isRequeue());
    assertEquals(watermark, trigger.getStatus().getTimestamp());
  }

  @Test
  void reconcileViewWithNoMatchingTriggersDoesNotRequeue() {
    OffsetDateTime watermark = OffsetDateTime.now();
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("view-no-triggers"))
        .status(new V1alpha1ViewStatus().watermark(watermark));
    views.add(view);

    Result result = reconciler.reconcile(new Request("ns", "view-no-triggers"));
    assertFalse(result.isRequeue());
  }

  @Test
  void reconcileViewFiresTriggerAndSetsTimestamp() {
    OffsetDateTime watermark = OffsetDateTime.now();
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("view-fires"))
        .status(new V1alpha1ViewStatus().watermark(watermark));
    views.add(view);

    Map<String, String> labels = new HashMap<>();
    labels.put("view", "view-fires");
    V1alpha1TableTrigger trigger1 = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("trigger-a").labels(labels));
    triggers.add(trigger1);

    Result result = reconciler.reconcile(new Request("ns", "view-fires"));
    assertFalse(result.isRequeue());
    assertNotNull(trigger1.getStatus());
    assertEquals(watermark, trigger1.getStatus().getTimestamp());
  }

  @Test
  void reconcileWithExceptionRequeuesWithFailureRetry() {
    // Use a view that will cause an exception during trigger processing
    OffsetDateTime watermark = OffsetDateTime.now();
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("error-view"))
        .status(new V1alpha1ViewStatus().watermark(watermark));
    views.add(view);

    // Create a trigger without metadata name to cause NPE during updateStatus
    V1alpha1TableTrigger badTrigger = new V1alpha1TableTrigger()
        .metadata(null);
    triggers.add(badTrigger);

    Result result = reconciler.reconcile(new Request("ns", "error-view"));
    assertTrue(result.isRequeue());
  }

  @Test
  void reconcileWithNon404SqlExceptionRequeues() throws SQLException {
    when(mockViewApi.get(anyString(), anyString()))
        .thenThrow(new SQLException("Server error", null, 500));
    ViewReconciler mockReconciler = new ViewReconciler(mockTriggerApi, mockViewApi);

    Result result = mockReconciler.reconcile(new Request("ns", "error-view"));

    assertTrue(result.isRequeue());
  }

  @Test
  @SuppressWarnings("unchecked")
  void controllerCreatesControllerFromContext() {
    SharedInformerFactory mockInformerFactory = mock(SharedInformerFactory.class);
    SharedIndexInformer<V1alpha1View> mockInformer = mock(SharedIndexInformer.class);
    when(mockInformerFactory.getExistingSharedIndexInformer(V1alpha1View.class)).thenReturn(mockInformer);
    K8sContext mockContext = mock(K8sContext.class);
    when(mockContext.informerFactory()).thenReturn(mockInformerFactory);

    Controller controller = ViewReconciler.controller(mockContext);

    assertNotNull(controller);
  }

  @Test
  void failureRetryDurationIsFiveMinutes() {
    assertEquals(Duration.ofMinutes(5), reconciler.failureRetryDuration());
  }

  @Test
  void pendingRetryDurationIsOneMinute() {
    assertEquals(Duration.ofMinutes(1), reconciler.pendingRetryDuration());
  }

  // if (status != null) — null status means triggers are NOT fired
  @Test
  void viewWithNullStatusDoesNotFireTriggers() {
    // View with null status
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("null-status-view"));
    views.add(view);

    // A trigger that would match if we incorrectly entered the trigger-fire block
    Map<String, String> labels = new HashMap<>();
    labels.put("view", "null-status-view");
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("should-not-fire").labels(labels));
    triggers.add(trigger);

    reconciler.reconcile(new Request("ns", "null-status-view"));

    // Trigger status must remain null — the block was not entered
    assertNull(trigger.getStatus());
  }

  // status != null but watermark == null — triggers NOT fired
  @Test
  void viewWithNullWatermarkDoesNotFireTriggers() {
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("null-watermark-view"))
        .status(new V1alpha1ViewStatus());  // status non-null, watermark null
    views.add(view);

    Map<String, String> labels = new HashMap<>();
    labels.put("view", "null-watermark-view");
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("should-not-fire-2").labels(labels));
    triggers.add(trigger);

    reconciler.reconcile(new Request("ns", "null-watermark-view"));

    assertNull(trigger.getStatus());
  }

  // tableTriggerApi.updateStatus is called
  @Test
  void updateStatusIsCalledWhenTriggerFires() {
    OffsetDateTime watermark = OffsetDateTime.now();
    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("update-status-view"))
        .status(new V1alpha1ViewStatus().watermark(watermark));
    views.add(view);

    Map<String, String> labels = new HashMap<>();
    labels.put("view", "update-status-view");
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("counted-trigger").labels(labels));

    List<V1alpha1TableTrigger> countedTriggers = new ArrayList<>();
    countedTriggers.add(trigger);

    int[] updateCount = {0};
    FakeK8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> countingTriggerApi =
        new FakeK8sApi<>(countedTriggers) {
          @Override
          public void updateStatus(V1alpha1TableTrigger obj, Object status) throws SQLException {
            updateCount[0]++;
            super.updateStatus(obj, status);
          }
        };

    ViewReconciler trackingReconciler = new ViewReconciler(
        countingTriggerApi,
        new FakeK8sApi<>(views));

    trackingReconciler.reconcile(new Request("ns", "update-status-view"));

    assertTrue(updateCount[0] > 0, "tableTriggerApi.updateStatus should have been called");
    assertEquals(watermark, trigger.getStatus().getTimestamp());
  }

  // trigger loop with existing (non-null) trigger status — sets timestamp
  @Test
  void existingTriggerStatusTimestampIsUpdated() {
    OffsetDateTime watermark = OffsetDateTime.now();
    OffsetDateTime oldTimestamp = watermark.minusDays(1);

    V1alpha1View view = new V1alpha1View()
        .metadata(new V1ObjectMeta().name("existing-status-view"))
        .status(new V1alpha1ViewStatus().watermark(watermark));
    views.add(view);

    Map<String, String> labels = new HashMap<>();
    labels.put("view", "existing-status-view");
    V1alpha1TableTriggerStatus existingStatus = new V1alpha1TableTriggerStatus()
        .timestamp(oldTimestamp);
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("existing-trigger").labels(labels))
        .status(existingStatus);
    triggers.add(trigger);

    reconciler.reconcile(new Request("ns", "existing-status-view"));

    // Timestamp must be updated from oldTimestamp to watermark
    assertEquals(watermark, trigger.getStatus().getTimestamp());
    assertFalse(oldTimestamp.equals(trigger.getStatus().getTimestamp()));
  }
}
