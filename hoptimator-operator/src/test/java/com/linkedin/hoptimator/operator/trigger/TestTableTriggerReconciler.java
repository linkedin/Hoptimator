package com.linkedin.hoptimator.operator.trigger;

import io.kubernetes.client.extended.controller.reconciler.Result;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTrigger;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTriggerStatus;


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
}
