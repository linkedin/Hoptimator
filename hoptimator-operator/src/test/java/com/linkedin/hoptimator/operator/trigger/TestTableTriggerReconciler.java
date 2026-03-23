package com.linkedin.hoptimator.operator.trigger;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.reconciler.Result;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.time.Duration;

import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobCondition;
import io.kubernetes.client.openapi.models.V1JobList;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.Yaml;

import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.FakeK8sYamlApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class TestTableTriggerReconciler {

  private List<V1Job> jobs = new ArrayList<>();
  private List<V1alpha1TableTrigger> triggers = new ArrayList<>();
  private Map<String, String> yamls = new HashMap<>();
  private final TableTriggerReconciler reconciler = new TableTriggerReconciler(
      new FakeK8sApi<V1alpha1TableTrigger, V1alpha1TableTriggerList>(triggers),
      new FakeK8sApi<V1Job, V1JobList>(jobs),
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
}
