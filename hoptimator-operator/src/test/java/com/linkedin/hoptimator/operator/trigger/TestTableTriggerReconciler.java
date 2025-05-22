package com.linkedin.hoptimator.operator.trigger;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  private final TableTriggerReconciler reconciler = new TableTriggerReconciler(null,
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
  void createsNewJob() {
    V1Job job = new V1Job().apiVersion("v1/batch").kind("Job")
        .metadata(new V1ObjectMeta().name("table-trigger-job"));
    triggers.add(new V1alpha1TableTrigger()
        .metadata(new V1ObjectMeta().name("table-trigger"))
        .spec(new V1alpha1TableTriggerSpec().yaml(Yaml.dump(job)))
        .status(new V1alpha1TableTriggerStatus().timestamp(OffsetDateTime.now())));
    reconciler.reconcile(new Request("namespace", "table-trigger"));
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
    reconciler.reconcile(new Request("namespace", "table-trigger"));
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
    reconciler.reconcile(new Request("namespace", "table-trigger"));
    Assertions.assertNotNull(trigger.getStatus().getWatermark(), "Watermark was not set");
  }
}
