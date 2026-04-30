package com.linkedin.hoptimator.operator.subscription;

import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineStatus;
import com.linkedin.hoptimator.k8s.models.V1alpha1Subscription;
import com.linkedin.hoptimator.k8s.models.V1alpha1SubscriptionList;
import com.linkedin.hoptimator.k8s.models.V1alpha1SubscriptionSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1SubscriptionStatus;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class SubscriptionReconcilerTest {

  private List<V1alpha1Subscription> subscriptions;
  private List<V1alpha1Pipeline> pipelines;
  private SubscriptionReconciler reconciler;
  private SubscriptionReconciler.Planner successPlanner;
  private SubscriptionReconciler.Planner failPlanner;

  private V1alpha1Subscription buildSubscription(String ns, String name, String sql) {
    V1alpha1Subscription sub = new V1alpha1Subscription();
    sub.setMetadata(new V1ObjectMeta().name(name).namespace(ns));
    sub.setSpec(new V1alpha1SubscriptionSpec().sql(sql).database("TESTDB"));
    return sub;
  }

  @BeforeEach
  void setUp() {
    subscriptions = new ArrayList<>();
    pipelines = new ArrayList<>();

    successPlanner = (sql, database, name, hints) ->
        new SubscriptionReconciler.PlanResult("INSERT INTO ...", List.of(
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\n  namespace: ns"));

    failPlanner = (sql, database, name, hints) -> {
      throw new RuntimeException("planner unavailable");
    };

    reconciler = new SubscriptionReconciler(null,
        new FakeK8sApi<>(subscriptions),
        new FakeK8sApi<>(pipelines),
        successPlanner);
  }

  // Deleted object
  @Test
  void deletedObjectDoesNotRequeue() {
    Result result = reconciler.reconcile(new Request("ns", "missing-sub"));
    assertFalse(result.isRequeue());
  }

  // Phase 1 – planning succeeds, status updated
  @Test
  void phase1PlansSuccessfully() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    subscriptions.add(sub);

    reconciler.reconcile(new Request("ns", "my-sub"));

    V1alpha1SubscriptionStatus status = sub.getStatus();
    assertNotNull(status);
    assertEquals("SELECT 1", status.getSql());
    assertFalse(status.getReady());
    assertFalse(status.getFailed());
    assertEquals("Planned.", status.getMessage());
    assertNotNull(status.getResources());
    assertFalse(status.getResources().isEmpty());
  }

  // Phase 1 – planning error marks subscription failed
  @Test
  void planningErrorMarksFailed() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    subscriptions.add(sub);

    SubscriptionReconciler failReconciler = new SubscriptionReconciler(null,
        new FakeK8sApi<>(subscriptions),
        new FakeK8sApi<>(pipelines),
        failPlanner);

    Result result = failReconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(result.isRequeue());
    assertTrue(sub.getStatus().getFailed());
    assertTrue(sub.getStatus().getMessage().startsWith("Error:"));
  }

  // Phase 2 – deploy Pipeline CR when it doesn't exist yet
  @Test
  void phase2DeploysPipelineWhenNotExists() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);
    status.setFailed(false);
    status.setResources(List.of("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: ns"));
    sub.setStatus(status);
    subscriptions.add(sub);

    // No pipeline exists → should deploy
    reconciler.reconcile(new Request("ns", "my-sub"));

    assertFalse(sub.getStatus().getReady());
    assertFalse(sub.getStatus().getFailed());
    assertEquals("Deployed.", sub.getStatus().getMessage());
  }

  // Phase 3 – Pipeline is ready → Subscription becomes ready
  @Test
  void phase3ReadyWhenPipelineReady() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);
    status.setResources(List.of("kind: ConfigMap"));
    sub.setStatus(status);
    subscriptions.add(sub);

    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("my-sub").namespace("ns"))
        .spec(new V1alpha1PipelineSpec().yaml("kind: Job\nmetadata:\n  name: job1"))
        .status(new V1alpha1PipelineStatus().ready(true).failed(false).message("Ready."));
    pipelines.add(pipeline);

    Result result = reconciler.reconcile(new Request("ns", "my-sub"));

    assertFalse(result.isRequeue());
    assertTrue(sub.getStatus().getReady());
    assertFalse(sub.getStatus().getFailed());
    assertEquals("Ready.", sub.getStatus().getMessage());
  }

  // Phase 3 – Pipeline failed → Subscription fails
  @Test
  void phase3FailedWhenPipelineFailed() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);
    status.setResources(List.of("kind: ConfigMap"));
    sub.setStatus(status);
    subscriptions.add(sub);

    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("my-sub").namespace("ns"))
        .spec(new V1alpha1PipelineSpec().yaml("kind: Job\nmetadata:\n  name: job1"))
        .status(new V1alpha1PipelineStatus().ready(false).failed(true).message("Job crashed."));
    pipelines.add(pipeline);

    Result result = reconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(result.isRequeue());
    assertFalse(sub.getStatus().getReady());
    assertTrue(sub.getStatus().getFailed());
    assertTrue(sub.getStatus().getMessage().contains("Job crashed."));
  }

  // Phase 3 – Pipeline not ready → requeue
  @Test
  void phase3RequeuesWhenPipelineNotReady() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);
    status.setResources(List.of("kind: ConfigMap"));
    sub.setStatus(status);
    subscriptions.add(sub);

    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("my-sub").namespace("ns"))
        .spec(new V1alpha1PipelineSpec().yaml("kind: Job\nmetadata:\n  name: job1"))
        .status(new V1alpha1PipelineStatus().ready(false).failed(false).message("Deployed."));
    pipelines.add(pipeline);

    Result result = reconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(result.isRequeue());
    assertFalse(sub.getStatus().getReady());
    assertFalse(sub.getStatus().getFailed());
    assertEquals("Deployed.", sub.getStatus().getMessage());
  }

  // Already ready → no requeue
  @Test
  void readySubscriptionDoesNotRequeue() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(true);
    status.setResources(List.of("kind: ConfigMap"));
    sub.setStatus(status);
    subscriptions.add(sub);

    Result result = reconciler.reconcile(new Request("ns", "my-sub"));

    assertFalse(result.isRequeue());
  }

  // diverged() – sql null in status triggers phase 1
  @Test
  void divergedWhenStatusSqlIsNull() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    subscriptions.add(sub);

    reconciler.reconcile(new Request("ns", "my-sub"));

    // Phase 1 was entered; status should have SQL
    assertEquals("SELECT 1", sub.getStatus().getSql());
  }

  // diverged() – sql and hints match → not diverged
  @Test
  void notDivergedWhenSqlAndHintsMatch() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    sub.getSpec().setHints(new HashMap<>());
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(true);
    status.setResources(List.of("kind: ConfigMap"));
    sub.setStatus(status);
    subscriptions.add(sub);

    Result result = reconciler.reconcile(new Request("ns", "my-sub"));

    // Not diverged, already ready → no requeue
    assertFalse(result.isRequeue());
  }

  // hints differ → diverged → re-plan
  @Test
  void divergedWhenHintsDiffer() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    sub.getSpec().setHints(Collections.singletonMap("key", "value"));
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(true);
    status.setResources(List.of("kind: ConfigMap"));
    sub.setStatus(status);
    subscriptions.add(sub);

    reconciler.reconcile(new Request("ns", "my-sub"));

    // Re-planned with new hints
    assertEquals(Collections.singletonMap("key", "value"), sub.getStatus().getHints());
    assertEquals("Planned.", sub.getStatus().getMessage());
  }

  // failureRetryDuration() returns exactly 5 minutes
  @Test
  void failureRetryDurationIsFiveMinutes() {
    assertEquals(Duration.ofMinutes(5), reconciler.failureRetryDuration());
  }

  // pendingRetryDuration() returns exactly 1 minute
  @Test
  void pendingRetryDurationIsOneMinute() {
    assertEquals(Duration.ofMinutes(1), reconciler.pendingRetryDuration());
  }

  // Phase 1 – hints are passed through
  @Test
  void phase1PassesHintsToPlanner() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    sub.getSpec().setHints(Collections.singletonMap("topicPrefix", "test"));
    subscriptions.add(sub);

    final boolean[] hintsSeen = {false};
    SubscriptionReconciler.Planner spyPlanner = (sql, database, name, hints) -> {
      hintsSeen[0] = hints != null && "test".equals(hints.get("topicPrefix"));
      return new SubscriptionReconciler.PlanResult("INSERT INTO ...", List.of("kind: ConfigMap"));
    };

    SubscriptionReconciler spyReconciler = new SubscriptionReconciler(null,
        new FakeK8sApi<>(subscriptions),
        new FakeK8sApi<>(pipelines),
        spyPlanner);

    spyReconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(hintsSeen[0], "Planner should receive hints from subscription spec");
  }

  // Outer exception handler – reconciliation exception → requeue
  @Test
  void outerExceptionHandlerRequeuesOnFailure() {
    // Subscription exists but subscriptionApi.updateStatus will throw
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    sub.getStatus();

    FakeK8sApi<V1alpha1Subscription, V1alpha1SubscriptionList> throwingApi =
        new FakeK8sApi<>(List.of(sub)) {
          @Override
          public void updateStatus(V1alpha1Subscription obj, Object status) throws java.sql.SQLException {
            throw new java.sql.SQLException("status update failed");
          }
        };

    SubscriptionReconciler throwingReconciler = new SubscriptionReconciler(null,
        throwingApi,
        new FakeK8sApi<>(pipelines),
        successPlanner);

    Result result = throwingReconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(result.isRequeue());
  }
}
