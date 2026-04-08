package com.linkedin.hoptimator.operator.trigger;

import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.FakeK8sYamlApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobCondition;
import io.kubernetes.client.openapi.models.V1JobList;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.Yaml;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class TableTriggerReconcilerTest {

  private final List<V1Job> jobs = new ArrayList<>();
  private final List<V1alpha1TableTrigger> triggers = new ArrayList<>();
  private final Map<String, String> yamls = new HashMap<>();
  private final TableTriggerReconciler reconciler = new TableTriggerReconciler(
      new FakeK8sApi<>(triggers),
      new FakeK8sApi<>(jobs),
      new FakeK8sYamlApi(yamls));

  @BeforeEach
  void beforeEach() {
    jobs.clear();
    triggers.clear();
    yamls.clear();
  }

  @Test
  void deletedJob() {
    Result result = reconciler.reconcile(new Request("namespace", "table-trigger"));
    Assertions.assertFalse(result.isRequeue());
    Assertions.assertTrue(yamls.isEmpty(), "Job should not exist");
  }

  @Test
  void createsNewJob() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("table-trigger-job").namespace("namespace"));
    triggers.add(new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("table-trigger"))
        .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now())));
    Result result = reconciler.reconcile(new Request("namespace", "table-trigger"));
    Assertions.assertTrue(result.isRequeue());
    Assertions.assertFalse(yamls.isEmpty(), "Job was not created");
  }

  @Test
  void deletesCompletedJob() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("completed-table-trigger-job"))
        .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Complete").status("True")));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("table-trigger"))
        .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
    triggers.add(trigger);
    jobs.add(job);
    Result result = reconciler.reconcile(new Request("namespace", "table-trigger"));
    Assertions.assertTrue(result.isRequeue());
    Assertions.assertTrue(jobs.isEmpty(), "Job was not deleted");
  }

  @Test
  void updatesWatermark() {
    Map<String, String> annotations = new HashMap<>();
    annotations.put("triggerTimestamp", OffsetDateTime.now().toString());
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("completed-table-trigger-job").annotations(annotations))
        .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Complete").status("True")));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("table-trigger"))
        .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
    triggers.add(trigger);
    jobs.add(job);
    Result result =  reconciler.reconcile(new Request("namespace", "table-trigger"));
    Assertions.assertTrue(result.isRequeue());
    Assertions.assertNotNull(trigger.getStatus().getWatermark(), "Watermark was not set");
  }

  @Test
  void reconcileTriggersAnnotationUpdate() {
    OffsetDateTime oldTimestamp = OffsetDateTime.parse("2024-01-01T12:00:00Z");
    OffsetDateTime newTimestamp = OffsetDateTime.parse("2024-01-03T12:00:00Z");
    Map<String, String> annotations = new HashMap<>();
    annotations.put(TableTriggerReconciler.TRIGGER_KEY, "table-trigger");
    annotations.put(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY, oldTimestamp.toString());
    V1Job job = new V1Job()
        .metadata(new V1ObjectMeta().name("running-table-trigger-job").annotations(annotations))
        .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Running").status("True")));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
      .metadata(new V1ObjectMeta().name("table-trigger"))
      .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
      .status(new V1alpha1TableTriggerStatus().timestamp(newTimestamp));
    triggers.add(trigger);
    jobs.add(job);
    reconciler.reconcile(new Request("namespace", "table-trigger"));
    Assertions.assertEquals(
      newTimestamp.toString(),
      Objects.requireNonNull(Objects.requireNonNull(job.getMetadata()).getAnnotations()).get(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY),
      "The annotation should be updated through reconcile when job is still running and if the trigger timestamp has advanced."
    );
  }

  @Test
  void firesTriggerOnSchedule() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("cron-trigger-job").namespace("namespace"));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("cron-trigger"))
        .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)).schedule("@hourly"));
    triggers.add(trigger);
    Result result = reconciler.reconcile(new Request("namespace", "cron-trigger"));
    Assertions.assertTrue(result.isRequeue());
    Assertions.assertNotNull(trigger.getStatus(), "Trigger was not fired: status null");
    Assertions.assertNotNull(trigger.getStatus().getTimestamp(), "Trigger was not fired");
  }

  @Test
  void doesNotFireTriggerWhenNoSchedule() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("cron-trigger-job").namespace("namespace"));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("cron-trigger"))
        .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)));
    triggers.add(trigger);
    Result result = reconciler.reconcile(new Request("namespace", "cron-trigger"));
    Assertions.assertFalse(result.isRequeue());
    Assertions.assertTrue(trigger.getStatus() == null || trigger.getStatus().getTimestamp() == null,
        "Trigger was fired when it shouldn't have been");
  }

  @Test
  void pausedTriggerDoesNotCreateNewJob() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("paused-trigger-job").namespace("namespace"));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("paused-trigger"))
        .spec(new V1alpha1TableTriggerSpec()
            .yaml(Yaml.dump(job))
            .paused(true))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
    triggers.add(trigger);
    Result result = reconciler.reconcile(new Request("namespace", "paused-trigger"));
    Assertions.assertFalse(result.isRequeue(), "Paused trigger should not requeue");
    Assertions.assertTrue(yamls.isEmpty(), "Paused trigger should not create job");
  }

  @Test
  void pausedTriggerDoesNotFireOnSchedule() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("paused-cron-trigger-job").namespace("namespace"));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("paused-cron-trigger"))
        .spec(new V1alpha1TableTriggerSpec()
            .yaml(Yaml.dump(job))
            .schedule("@hourly")
            .paused(true));
    triggers.add(trigger);
    Result result = reconciler.reconcile(new Request("namespace", "paused-cron-trigger"));
    Assertions.assertFalse(result.isRequeue(), "Paused trigger should not requeue for schedule");
    Assertions.assertTrue(trigger.getStatus() == null || trigger.getStatus().getTimestamp() == null,
        "Paused trigger should not be fired on schedule");
    Assertions.assertTrue(yamls.isEmpty(), "Paused trigger should not create job on schedule");
  }

  @Test
  void pausedTriggerMonitorsExistingRunningJob() {
    Map<String, String> annotations = new HashMap<>();
    annotations.put(TableTriggerReconciler.TRIGGER_KEY, "paused-trigger-with-job");
    annotations.put(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY, OffsetDateTime.now().toString());
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("paused-trigger-running-job").annotations(annotations))
        .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Running").status("True")));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("paused-trigger-with-job"))
        .spec(new V1alpha1TableTriggerSpec()
            .yaml(Yaml.dump(job))
            .paused(true))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
    triggers.add(trigger);
    jobs.add(job);
    Result result = reconciler.reconcile(new Request("namespace", "paused-trigger-with-job"));
    Assertions.assertTrue(result.isRequeue(), "Paused trigger should requeue to monitor existing job");
    Assertions.assertFalse(jobs.isEmpty(), "Existing job should still be running");
  }

  @Test
  void pausedTriggerDeletesCompletedJob() {
    Map<String, String> annotations = new HashMap<>();
    annotations.put(TableTriggerReconciler.TRIGGER_KEY, "paused-trigger-completed");
    annotations.put(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY, OffsetDateTime.now().toString());
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("paused-trigger-completed-job").annotations(annotations))
        .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Complete").status("True")));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("paused-trigger-completed"))
        .spec(new V1alpha1TableTriggerSpec()
            .yaml(Yaml.dump(job))
            .paused(true))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
    triggers.add(trigger);
    jobs.add(job);
    Result result = reconciler.reconcile(new Request("namespace", "paused-trigger-completed"));
    Assertions.assertTrue(result.isRequeue(), "Should requeue after handling completed job");
    Assertions.assertTrue(jobs.isEmpty(), "Completed job should be deleted even when trigger is paused");
    Assertions.assertNotNull(trigger.getStatus().getWatermark(), "Watermark should be updated for completed job");
  }

  @Test
  void unpausedTriggerCreatesJobAfterBeingPaused() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("unpaused-trigger-job").namespace("namespace"));
    OffsetDateTime triggerTime = OffsetDateTime.now().minusHours(1);
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("unpaused-trigger"))
        .spec(new V1alpha1TableTriggerSpec()
            .yaml(Yaml.dump(job))
            .paused(false))
        .status(new V1alpha1TableTriggerStatus()
            .timestamp(triggerTime));  // Has timestamp but no watermark (was paused)
    triggers.add(trigger);
    Result result = reconciler.reconcile(new Request("namespace", "unpaused-trigger"));
    Assertions.assertTrue(result.isRequeue(), "Unpaused trigger should requeue");
    Assertions.assertFalse(yamls.isEmpty(), "Unpaused trigger should create job for pending trigger event");
  }

  @Test
  void triggerWithNullYamlDoesNotRequeue() {
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("no-yaml-trigger"))
        .spec(new V1alpha1TableTriggerSpec());
    triggers.add(trigger);
    Result result = reconciler.reconcile(new Request("namespace", "no-yaml-trigger"));
    Assertions.assertFalse(result.isRequeue());
    Assertions.assertTrue(yamls.isEmpty());
  }

  @Test
  void deletesFailedJob() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("failed-trigger-job"))
        .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Failed").status("True")));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("table-trigger"))
        .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
    triggers.add(trigger);
    jobs.add(job);
    Result result = reconciler.reconcile(new Request("namespace", "table-trigger"));
    Assertions.assertTrue(result.isRequeue());
    Assertions.assertTrue(jobs.isEmpty(), "Failed job was not deleted");
  }

  @Test
  void jobWithNoStatusRequeuesForLater() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("no-status-job"));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("table-trigger"))
        .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
    triggers.add(trigger);
    jobs.add(job);
    Result result = reconciler.reconcile(new Request("namespace", "table-trigger"));
    Assertions.assertTrue(result.isRequeue());
    Assertions.assertFalse(jobs.isEmpty(), "Job should still exist");
  }

  @Test
  void completedJobWithNoTimestampAnnotationDoesNotAdvanceWatermark() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("no-annotation-job"))
        .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Complete").status("True")));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("table-trigger"))
        .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
    triggers.add(trigger);
    jobs.add(job);
    Result result = reconciler.reconcile(new Request("namespace", "table-trigger"));
    Assertions.assertTrue(result.isRequeue());
    Assertions.assertTrue(jobs.isEmpty(), "Completed job should still be deleted");
    Assertions.assertNull(trigger.getStatus().getWatermark(), "Watermark should not be advanced without annotation");
  }

  @Test
  void failureRetryDurationIsFiveMinutes() {
    Assertions.assertEquals(Duration.ofMinutes(5), reconciler.failureRetryDuration());
  }

  @Test
  void pendingRetryDurationIsOneMinute() {
    Assertions.assertEquals(Duration.ofMinutes(1), reconciler.pendingRetryDuration());
  }

  @Test
  void maybeUpdateJobAnnotationDoesNothingWhenTimestampIsOlder() throws Exception {
    OffsetDateTime existingTimestamp = OffsetDateTime.parse("2024-06-01T12:00:00Z");
    OffsetDateTime olderTimestamp = OffsetDateTime.parse("2024-01-01T12:00:00Z");
    Map<String, String> annotations = new HashMap<>();
    annotations.put(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY, existingTimestamp.toString());
    V1Job job = new V1Job()
        .metadata(new V1ObjectMeta().name("test-job").annotations(annotations));
    reconciler.maybeUpdateJobAnnotation(job, olderTimestamp);
    Assertions.assertEquals(existingTimestamp.toString(),
        job.getMetadata().getAnnotations().get(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY),
        "Annotation should not be updated when new timestamp is older");
  }

  @Test
  void maybeUpdateJobAnnotationDoesNothingWhenAnnotationsNull() throws Exception {
    V1Job job = new V1Job()
        .metadata(new V1ObjectMeta().name("test-job"));
    // Should not throw
    reconciler.maybeUpdateJobAnnotation(job, OffsetDateTime.now());
  }

  @Test
  void jobWithNoConditionsButHasStatusRequeues() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("empty-conditions-job"))
        .status(new V1JobStatus());
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("table-trigger"))
        .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
    triggers.add(trigger);
    jobs.add(job);
    Result result = reconciler.reconcile(new Request("namespace", "table-trigger"));
    Assertions.assertTrue(result.isRequeue());
  }

  @Test
  void scheduledTriggerWithWatermarkEqualToTimestampSleeps() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("sleeping-trigger-job").namespace("namespace"));
    OffsetDateTime now = OffsetDateTime.now();
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("sleeping-trigger"))
        .spec(new V1alpha1TableTriggerSpec()
            .yaml(Yaml.dump(job))
            .schedule("@hourly"))
        .status(new V1alpha1TableTriggerStatus()
            .timestamp(now)
            .watermark(now));
    triggers.add(trigger);
    Result result = reconciler.reconcile(new Request("namespace", "sleeping-trigger"));
    Assertions.assertTrue(result.isRequeue(), "Should requeue to wait for next scheduled execution");
  }

  @Test
  void pausedTriggerWithNullStatusAndExistingJobDoesNotCrash() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("paused-null-status-job").namespace("namespace"));
    V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("paused-null-status"))
        .spec(new V1alpha1TableTriggerSpec()
            .yaml(Yaml.dump(job))
            .paused(true));
    triggers.add(trigger);
    Result result = reconciler.reconcile(new Request("namespace", "paused-null-status"));
    Assertions.assertFalse(result.isRequeue());
  }

  @Test
  @SuppressWarnings("unchecked")
  void controllerCreatesControllerFromContext() {
    SharedInformerFactory mockInformerFactory = mock(SharedInformerFactory.class);
    SharedIndexInformer<V1alpha1TableTrigger> mockInformer = mock(SharedIndexInformer.class);
    when(mockInformerFactory.getExistingSharedIndexInformer(V1alpha1TableTrigger.class)).thenReturn(mockInformer);
    K8sContext mockContext = mock(K8sContext.class);
    when(mockContext.informerFactory()).thenReturn(mockInformerFactory);

    Controller controller = TableTriggerReconciler.controller(mockContext);

    Assertions.assertNotNull(controller);
  }

  // -----------------------------------------------------------------------
  // Interaction-based tests using spy-wrapped FakeK8sApi
  // -----------------------------------------------------------------------

  @Nested
  static class InteractionTests {

    private final List<V1Job> jobs = new ArrayList<>();
    private final List<V1alpha1TableTrigger> triggers = new ArrayList<>();
    private final Map<String, String> yamls = new HashMap<>();

    private FakeK8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> triggerApiSpy;
    private FakeK8sApi<V1Job, V1JobList> jobApiSpy;
    private FakeK8sYamlApi yamlApiSpy;

    private TableTriggerReconciler reconciler;

    /**
     * Build a minimal valid V1Job YAML string directly (avoiding Yaml.dump(DynamicKubernetesObject) issues
     * in FakeK8sYamlApi.createWithMetadata when the job is stored back).
     * The job needs apiVersion + kind for Dynamics.newFromYaml to work correctly.
     */
    private static V1Job jobWithApiVersion(String name, String namespace) {
      return new V1Job().apiVersion("batch/v1").kind("Job")
          .metadata(new V1ObjectMeta().name(name).namespace(namespace));
    }

    @BeforeEach
    void setUp() {
      jobs.clear();
      triggers.clear();
      yamls.clear();
      triggerApiSpy = spy(new FakeK8sApi<>(triggers));
      jobApiSpy = spy(new FakeK8sApi<>(jobs));
      yamlApiSpy = spy(new FakeK8sYamlApi(yamls));
      reconciler = new TableTriggerReconciler(triggerApiSpy, jobApiSpy, yamlApiSpy);
    }

    @Test
    void completedJobCallsUpdateStatusOnTriggerApi() throws Exception {
      OffsetDateTime ts = OffsetDateTime.parse("2024-05-01T10:00:00Z");
      Map<String, String> annotations = new HashMap<>();
      annotations.put(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY, ts.toString());
      V1Job job = jobWithApiVersion("completed-job", "ns")
          .metadata(new V1ObjectMeta().name("completed-job").namespace("ns").annotations(annotations))
          .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Complete").status("True")));
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("my-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(ts));
      triggers.add(trigger);
      jobs.add(job);

      reconciler.reconcile(new Request("ns", "my-trigger"));

      // Verify updateStatus is actually called
      verify(triggerApiSpy, times(1)).updateStatus(eq(trigger), any());
      // The watermark must reflect the job's timestamp annotation
      assertEquals(ts, trigger.getStatus().getWatermark());
    }

    @Test
    void completedJobWithNoAnnotationDoesNotCallUpdateStatus() throws Exception {
      V1Job job = jobWithApiVersion("no-annotation-completed-job", "ns")
          .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Complete").status("True")));
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("no-annotation-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
      triggers.add(trigger);
      jobs.add(job);

      reconciler.reconcile(new Request("ns", "no-annotation-trigger"));

      // When annotation is missing, updateStatus should NOT be called
      verify(triggerApiSpy, never()).updateStatus(any(), any());
    }

    // removes tableTriggerApi.updateStatus after schedule fire
    @Test
    void scheduleFiringCallsUpdateStatusOnTriggerApi() throws Exception {
      V1Job job = jobWithApiVersion("sched-job", "ns");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("sched-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)).schedule("@hourly"));
      triggers.add(trigger);

      Result result = reconciler.reconcile(new Request("ns", "sched-trigger"));

      assertTrue(result.isRequeue());
      // Verify updateStatus is called when schedule fires
      verify(triggerApiSpy, times(1)).updateStatus(eq(trigger), any());
      assertNotNull(trigger.getStatus().getTimestamp());
    }

    @Test
    void scheduleAlreadyFiredRecentlyDoesNotCallUpdateStatus() throws Exception {
      // Timestamp == last schedule execution (watermark == timestamp), so no new fire
      V1Job job = jobWithApiVersion("sched-noupdate-job", "ns");
      OffsetDateTime now = OffsetDateTime.now();
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("sched-noupdate-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)).schedule("@hourly"))
          .status(new V1alpha1TableTriggerStatus().timestamp(now).watermark(now));
      triggers.add(trigger);

      reconciler.reconcile(new Request("ns", "sched-noupdate-trigger"));

      // No job created, no new fire — updateStatus should NOT be called
      verify(triggerApiSpy, never()).updateStatus(any(), any());
    }

    @Test
    void maybeUpdateJobAnnotationUpdatesAnnotationMapAndCallsJobApiUpdate() throws Exception {
      OffsetDateTime oldTs = OffsetDateTime.parse("2024-01-01T00:00:00Z");
      OffsetDateTime newTs = OffsetDateTime.parse("2024-06-01T00:00:00Z");
      Map<String, String> annotations = new HashMap<>();
      annotations.put(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY, oldTs.toString());
      V1Job job = new V1Job()
          .metadata(new V1ObjectMeta().name("job-to-annotate").annotations(annotations));
      jobs.add(job);

      reconciler.maybeUpdateJobAnnotation(job, newTs);

      // verify annotation map was updated in place
      assertEquals(newTs.toString(), job.getMetadata().getAnnotations().get(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY),
          "Annotation should be updated to new timestamp");
      // verify jobApi.update() was actually called
      verify(jobApiSpy, times(1)).update(job);
    }

    @Test
    void maybeUpdateJobAnnotationWithOlderTimestampDoesNotCallUpdate() throws Exception {
      OffsetDateTime existingTs = OffsetDateTime.parse("2024-06-01T00:00:00Z");
      OffsetDateTime olderTs = OffsetDateTime.parse("2024-01-01T00:00:00Z");
      Map<String, String> annotations = new HashMap<>();
      annotations.put(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY, existingTs.toString());
      V1Job job = new V1Job()
          .metadata(new V1ObjectMeta().name("job-older").annotations(annotations));
      jobs.add(job);

      reconciler.maybeUpdateJobAnnotation(job, olderTs);

      // Older timestamp — should not update annotation or call jobApi.update
      assertEquals(existingTs.toString(), job.getMetadata().getAnnotations().get(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY));
      verify(jobApiSpy, never()).update(any());
    }

    @Test
    void maybeUpdateJobAnnotationWithNullAnnotationsDoesNotCallUpdate() throws Exception {
      V1Job job = new V1Job()
          .metadata(new V1ObjectMeta().name("job-no-annotations"));
      jobs.add(job);

      reconciler.maybeUpdateJobAnnotation(job, OffsetDateTime.now());

      verify(jobApiSpy, never()).update(any());
    }

    // Test that jobProperties are included in template rendering context.
    @Test
    void jobPropertiesArePassedToTemplateRendering() throws Exception {
      // Template with a placeholder for a custom property
      String yamlTemplate = "apiVersion: batch/v1\nkind: Job\nmetadata:\n"
          + "  name: prop-job\n  namespace: ns\nspec:\n  template:\n    spec:\n"
          + "      containers:\n      - name: c\n        env:\n"
          + "        - name: MY_PROP\n          value: {{myprop}}\n";
      Map<String, String> jobProps = new HashMap<>();
      jobProps.put("myprop", "hello-world");
      OffsetDateTime ts = OffsetDateTime.parse("2024-01-01T12:00:00Z");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("prop-trigger").namespace("ns"))
          .spec(new V1alpha1TableTriggerSpec()
              .yaml(yamlTemplate)
              .schema("myschema")
              .table("mytable")
              .jobProperties(jobProps))
          .status(new V1alpha1TableTriggerStatus().timestamp(ts));
      triggers.add(trigger);

      reconciler.reconcile(new Request("ns", "prop-trigger"));

      // Verify createWithMetadata was called (job creation was attempted)
      verify(yamlApiSpy, atLeastOnce()).createWithMetadata(anyString(), anyMap(), anyMap(), any());
      // The rendered YAML argument should contain the property value
      // Use ArgumentCaptor to check the actual YAML string passed to createWithMetadata
      ArgumentCaptor<String> yamlCaptor = forClass(String.class);
      verify(yamlApiSpy).createWithMetadata(yamlCaptor.capture(), anyMap(), anyMap(), any());
      assertTrue(yamlCaptor.getValue().contains("hello-world"),
          "Job YAML passed to createWithMetadata should contain rendered property value");
    }

    @Test
    void nullJobPropertiesDoesNotPreventJobCreation() throws Exception {
      // When jobProperties is null, job creation should still proceed (no NPE)
      V1Job job = jobWithApiVersion("noprop-job", "ns");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("noprop-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
      // jobProperties is null by default
      triggers.add(trigger);

      Result result = reconciler.reconcile(new Request("ns", "noprop-trigger"));

      assertTrue(result.isRequeue(), "Should create a job and requeue");
      // Verify createWithMetadata was called with the job YAML
      verify(yamlApiSpy, times(1)).createWithMetadata(anyString(), anyMap(), anyMap(), any());
    }

    @Test
    void noJobAndWatermarkNullTriggersJobCreation() throws Exception {
      V1Job job = jobWithApiVersion("wm-null-job", "ns");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("wm-null-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
      // watermark is null (not set)
      triggers.add(trigger);

      Result result = reconciler.reconcile(new Request("ns", "wm-null-trigger"));

      assertTrue(result.isRequeue());
      // Verify job creation was attempted
      verify(yamlApiSpy, times(1)).createWithMetadata(anyString(), anyMap(), anyMap(), any());
    }

    @Test
    void noJobAndTimestampAfterWatermarkTriggersJobCreation() throws Exception {
      V1Job job = jobWithApiVersion("ts-after-wm-job", "ns");
      OffsetDateTime watermark = OffsetDateTime.parse("2024-01-01T00:00:00Z");
      OffsetDateTime timestamp = OffsetDateTime.parse("2024-06-01T00:00:00Z");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("ts-after-wm-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(timestamp).watermark(watermark));
      triggers.add(trigger);

      Result result = reconciler.reconcile(new Request("ns", "ts-after-wm-trigger"));

      assertTrue(result.isRequeue());
      // Verify job creation was attempted — timestamp > watermark, so job should be created
      verify(yamlApiSpy, times(1)).createWithMetadata(anyString(), anyMap(), anyMap(), any());
    }

    @Test
    void noJobAndWatermarkEqualsTimestampDoesNotCreateJob() throws Exception {
      V1Job job = jobWithApiVersion("wm-eq-ts-job", "ns");
      OffsetDateTime ts = OffsetDateTime.parse("2024-06-01T00:00:00Z");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("wm-eq-ts-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(ts).watermark(ts));
      triggers.add(trigger);

      reconciler.reconcile(new Request("ns", "wm-eq-ts-trigger"));

      // watermark == timestamp — no job should be created
      verify(yamlApiSpy, never()).createWithMetadata(anyString(), anyMap(), anyMap(), any());
    }

    // spec.getYaml() == null check
    @Test
    void nullYamlSpecReturnsNoRequeue() {
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("null-yaml"))
          .spec(new V1alpha1TableTriggerSpec());  // yaml is null
      triggers.add(trigger);

      Result result = reconciler.reconcile(new Request("ns", "null-yaml"));

      assertFalse(result.isRequeue());
      assertTrue(yamls.isEmpty());
    }

    @Test
    void triggerWithTimestampInStatusCreatesJob() throws Exception {
      V1Job job = jobWithApiVersion("timestamped-job", "ns");
      OffsetDateTime ts = OffsetDateTime.now().minusHours(2);
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("timestamped-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(ts));
      triggers.add(trigger);

      Result result = reconciler.reconcile(new Request("ns", "timestamped-trigger"));

      // Job should be created (watermark is null so job creation proceeds)
      assertTrue(result.isRequeue());
      verify(yamlApiSpy, times(1)).createWithMetadata(anyString(), anyMap(), anyMap(), any());
    }

    @Test
    void triggerWithNullTimestampInStatusRequeuesLater() throws Exception {
      // status exists but timestamp is null — status.getTimestamp() == null branch
      V1Job job = jobWithApiVersion("no-ts-job", "ns");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("no-ts-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus());  // status with null timestamp
      triggers.add(trigger);

      Result result = reconciler.reconcile(new Request("ns", "no-ts-trigger"));

      // No timestamp and no schedule: falls to final else — requeue for later
      assertTrue(result.isRequeue(), "Should requeue for later (no timestamp case)");
      // No job should be created when timestamp is null
      verify(yamlApiSpy, never()).createWithMetadata(anyString(), anyMap(), anyMap(), any());
    }

    // 404 check vs other SQL exceptions
    @Test
    void nonFourOhFourSqlExceptionRequeuesWithDelay() {
      FakeK8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> failingApi =
          new FakeK8sApi<>(triggers) {
            @Override
            public V1alpha1TableTrigger get(String namespace, String name) throws SQLException {
              throw new SQLException("DB error", null, 500);
            }
          };
      TableTriggerReconciler failingReconciler = new TableTriggerReconciler(
          failingApi, jobApiSpy, yamlApiSpy);

      // 500 error is not 404, so it propagates to the outer catch and requeues with failure delay
      Result result = failingReconciler.reconcile(new Request("ns", "any-trigger"));

      assertTrue(result.isRequeue(), "Non-404 error should requeue");
    }

    @Test
    void fourOhFourSqlExceptionReturnsNoRequeue() {
      FakeK8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList> notFoundApi =
          new FakeK8sApi<>(triggers) {
            @Override
            public V1alpha1TableTrigger get(String namespace, String name) throws SQLException {
              throw new SQLException("Not found", null, 404);
            }
          };
      TableTriggerReconciler notFoundReconciler = new TableTriggerReconciler(
          notFoundApi, jobApiSpy, yamlApiSpy);

      Result result = notFoundReconciler.reconcile(new Request("ns", "deleted-trigger"));

      assertFalse(result.isRequeue(), "404 should not requeue");
    }

    @Test
    void failedJobConditionDeletesJobAndRequeues() throws Exception {
      V1Job job = jobWithApiVersion("failed-job", "ns")
          .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Failed").status("True")));
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("failed-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
      triggers.add(trigger);
      jobs.add(job);

      Result result = reconciler.reconcile(new Request("ns", "failed-trigger"));

      assertTrue(result.isRequeue());
      assertTrue(jobs.isEmpty(), "Failed job must be deleted");
      // For failed job, updateStatus should NOT be called
      verify(triggerApiSpy, never()).updateStatus(any(), any());
    }

    @Test
    void completeConditionDeletesJobAdvancesWatermarkAndCallsUpdateStatus() throws Exception {
      OffsetDateTime ts = OffsetDateTime.parse("2024-05-15T08:00:00Z");
      Map<String, String> annotations = new HashMap<>();
      annotations.put(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY, ts.toString());
      V1Job job = jobWithApiVersion("complete-job", "ns")
          .metadata(new V1ObjectMeta().name("complete-job").namespace("ns").annotations(annotations))
          .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Complete").status("True")));
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("complete-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(ts));
      triggers.add(trigger);
      jobs.add(job);

      Result result = reconciler.reconcile(new Request("ns", "complete-trigger"));

      assertTrue(result.isRequeue());
      assertTrue(jobs.isEmpty(), "Completed job should be deleted");
      assertEquals(ts, trigger.getStatus().getWatermark(), "Watermark must be set to job timestamp");
      verify(triggerApiSpy, times(1)).updateStatus(eq(trigger), any());
    }

    @Test
    void runningJobRequeuesWithDelay() {
      V1Job job = jobWithApiVersion("running-job", "ns")
          .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Running").status("True")));
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("running-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
      triggers.add(trigger);
      jobs.add(job);

      Result result = reconciler.reconcile(new Request("ns", "running-trigger"));

      assertTrue(result.isRequeue(), "Running job should requeue for later check");
      assertFalse(jobs.isEmpty(), "Running job should not be deleted");
    }

    @Test
    void jobWithNullConditionsRequeuesWithDelay() {
      // job.getStatus() != null but getConditions() is null
      V1Job job = jobWithApiVersion("null-conditions-job", "ns")
          .status(new V1JobStatus());  // status != null, conditions == null
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("null-conds-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
      triggers.add(trigger);
      jobs.add(job);

      Result result = reconciler.reconcile(new Request("ns", "null-conds-trigger"));

      assertTrue(result.isRequeue());
    }

    @Test
    void jobWithNullJobStatusRequeuesWithDelay() {
      // job.getStatus() == null
      V1Job job = jobWithApiVersion("null-status-job", "ns");  // status == null
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("null-status-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now()));
      triggers.add(trigger);
      jobs.add(job);

      Result result = reconciler.reconcile(new Request("ns", "null-status-trigger"));

      assertTrue(result.isRequeue());
    }

    // scheduled condition and timestamp comparison
    @Test
    void scheduledTriggerWithNoTimestampFiresAndSetsTimestamp() throws Exception {
      V1Job job = jobWithApiVersion("first-fire-job", "ns");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("first-fire-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)).schedule("@hourly"))
          .status(new V1alpha1TableTriggerStatus());  // status with null timestamp
      triggers.add(trigger);

      Result result = reconciler.reconcile(new Request("ns", "first-fire-trigger"));

      assertTrue(result.isRequeue());
      assertNotNull(trigger.getStatus().getTimestamp(), "Trigger should be fired (timestamp set)");
      verify(triggerApiSpy, times(1)).updateStatus(eq(trigger), any());
    }

    @Test
    void scheduledTriggerWithOldTimestampRefiresToNewerExecution() throws Exception {
      V1Job job = jobWithApiVersion("after-sched-job", "ns");
      // Set timestamp to far in the past — it IS before last schedule execution, so it will fire again
      OffsetDateTime pastTimestamp = OffsetDateTime.parse("2020-01-01T00:00:00Z");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("after-sched-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)).schedule("@hourly"))
          .status(new V1alpha1TableTriggerStatus().timestamp(pastTimestamp));
      triggers.add(trigger);

      Result result = reconciler.reconcile(new Request("ns", "after-sched-trigger"));

      assertTrue(result.isRequeue());
      // timestamp should be updated to a more recent execution time
      assertTrue(trigger.getStatus().getTimestamp().isAfter(pastTimestamp),
          "Timestamp should be updated to more recent schedule execution time");
      verify(triggerApiSpy, times(1)).updateStatus(any(), any());
    }

    // sanity check (status.getTimestamp() == null path)
    @Test
    void reconcileWithoutScheduleAndNullStatusReturnsNoRequeue() {
      V1Job job = jobWithApiVersion("no-sched-no-ts", "ns");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("no-sched-no-ts-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)));  // no schedule, no status
      triggers.add(trigger);

      Result result = reconciler.reconcile(new Request("ns", "no-sched-no-ts-trigger"));

      assertFalse(result.isRequeue(), "Trigger with no schedule and no status should not requeue");
    }

    // job != null branches (else-if chain)
    @Test
    void existingJobWithScheduleTriggerHandlesJobFirst() {
      V1Job job = jobWithApiVersion("exists-sched-job", "ns")
          .status(new V1JobStatus());  // no conditions
      OffsetDateTime ts = OffsetDateTime.now();
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("exists-sched-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)).schedule("@hourly"))
          .status(new V1alpha1TableTriggerStatus().timestamp(ts).watermark(ts));
      triggers.add(trigger);
      jobs.add(job);

      Result result = reconciler.reconcile(new Request("ns", "exists-sched-trigger"));

      // job != null triggers handleExistingJob(), not the schedule path
      assertTrue(result.isRequeue(), "Should requeue when job exists");
    }

    // -----------------------------------------------------------------------
    // pausedTrigger with existing completed job — verify updateStatus is called
    // -----------------------------------------------------------------------

    @Test
    void pausedTriggerWithCompletedJobCallsUpdateStatus() throws Exception {
      OffsetDateTime ts = OffsetDateTime.parse("2024-03-01T06:00:00Z");
      Map<String, String> annotations = new HashMap<>();
      annotations.put(TableTriggerReconciler.TRIGGER_TIMESTAMP_KEY, ts.toString());
      V1Job job = jobWithApiVersion("paused-done-job", "ns")
          .metadata(new V1ObjectMeta().name("paused-done-job").namespace("ns").annotations(annotations))
          .status(new V1JobStatus().addConditionsItem(new V1JobCondition().type("Complete").status("True")));
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("paused-done-trigger"))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)).paused(true))
          .status(new V1alpha1TableTriggerStatus().timestamp(ts));
      triggers.add(trigger);
      jobs.add(job);

      Result result = reconciler.reconcile(new Request("ns", "paused-done-trigger"));

      assertTrue(result.isRequeue());
      verify(triggerApiSpy, times(1)).updateStatus(eq(trigger), any());
      assertEquals(ts, trigger.getStatus().getWatermark());
    }

    @Test
    void jobCreatedWithSyntheticOwnerReferenceWhenTriggerHasNone() throws Exception {
      // When the trigger has no ownerReferences, the reconciler should synthesise one
      // pointing back to the trigger itself (apiVersion/kind/name/uid).
      V1Job job = jobWithApiVersion("owner-ref-job", "ns");
      OffsetDateTime ts = OffsetDateTime.parse("2024-01-01T00:00:00Z");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .apiVersion("hoptimator.linkedin.com/v1alpha1")
          .kind("TableTrigger")
          .metadata(new V1ObjectMeta().name("ownerless-trigger").uid("trigger-uid-abc"))
          // no ownerReferences set
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(ts));
      triggers.add(trigger);

      reconciler.reconcile(new Request("ns", "ownerless-trigger"));

      @SuppressWarnings("unchecked")
      ArgumentCaptor<List<V1OwnerReference>> ownerRefCaptor = forClass((Class) List.class);
      verify(yamlApiSpy).createWithMetadata(anyString(), anyMap(), anyMap(), ownerRefCaptor.capture());
      List<V1OwnerReference> ownerRefs = ownerRefCaptor.getValue();
      assertEquals(1, ownerRefs.size());
      V1OwnerReference ref = ownerRefs.get(0);
      assertEquals("hoptimator.linkedin.com/v1alpha1", ref.getApiVersion());
      assertEquals("TableTrigger", ref.getKind());
      assertEquals("ownerless-trigger", ref.getName());
      assertEquals("trigger-uid-abc", ref.getUid());
    }

    @Test
    void jobCreatedWithExistingOwnerReferencesWhenTriggerHasThem() throws Exception {
      // When the trigger already has ownerReferences, they should be passed through unchanged.
      V1Job job = jobWithApiVersion("existing-owner-ref-job", "ns");
      OffsetDateTime ts = OffsetDateTime.parse("2024-01-01T00:00:00Z");
      V1OwnerReference existingRef = new V1OwnerReference()
          .apiVersion("apps/v1")
          .kind("Deployment")
          .name("parent-deployment")
          .uid("parent-uid-xyz");
      V1alpha1TableTrigger trigger = new V1alpha1TableTrigger()
          .metadata(new V1ObjectMeta().name("owned-trigger")
              .ownerReferences(List.of(existingRef)))
          .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
          .status(new V1alpha1TableTriggerStatus().timestamp(ts));
      triggers.add(trigger);

      reconciler.reconcile(new Request("ns", "owned-trigger"));

      @SuppressWarnings("unchecked")
      ArgumentCaptor<List<V1OwnerReference>> ownerRefCaptor = forClass((Class) List.class);
      verify(yamlApiSpy).createWithMetadata(anyString(), anyMap(), anyMap(), ownerRefCaptor.capture());
      List<V1OwnerReference> ownerRefs = ownerRefCaptor.getValue();
      assertEquals(1, ownerRefs.size());
      assertEquals(existingRef, ownerRefs.get(0));
    }
  }
}
