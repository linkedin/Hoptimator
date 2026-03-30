package com.linkedin.hoptimator.operator.pipeline;

import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineStatus;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatus;
import com.linkedin.hoptimator.k8s.status.K8sPipelineElementStatusEstimator;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class PipelineReconcilerTest {

  @Mock
  private K8sPipelineElementStatusEstimator elementStatusEstimator;

  private List<V1alpha1Pipeline> pipelines;
  private PipelineReconciler reconciler;

  @BeforeEach
  void setUp() {
    pipelines = new ArrayList<>();
    reconciler = new PipelineReconciler(null,
        new FakeK8sApi<V1alpha1Pipeline, V1alpha1PipelineList>(pipelines),
        elementStatusEstimator);
  }

  @Test
  void reconcileDeletedPipelineDoesNotRequeue() {
    Result result = reconciler.reconcile(new Request("ns", "deleted-pipeline"));
    assertFalse(result.isRequeue());
  }

  @Test
  void reconcileReadyPipelineDoesNotRequeue() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("ready-pipeline").namespace("ns"))
        .spec(new V1alpha1PipelineSpec().yaml("kind: Job\nmetadata:\n  name: job1"))
        .status(new V1alpha1PipelineStatus());
    pipelines.add(pipeline);

    K8sPipelineElementStatus readyStatus = new K8sPipelineElementStatus("Job/job1", true, false, "Ready.");
    when(elementStatusEstimator.estimateStatuses(any())).thenReturn(Collections.singletonList(readyStatus));

    Result result = reconciler.reconcile(new Request("ns", "ready-pipeline"));
    assertFalse(result.isRequeue());
    assertTrue(pipeline.getStatus().getReady());
    assertFalse(pipeline.getStatus().getFailed());
    assertEquals("Ready.", pipeline.getStatus().getMessage());
  }

  @Test
  void reconcileFailedPipelineRequeues() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("failed-pipeline").namespace("ns"))
        .spec(new V1alpha1PipelineSpec().yaml("kind: Job\nmetadata:\n  name: job1"))
        .status(new V1alpha1PipelineStatus());
    pipelines.add(pipeline);

    K8sPipelineElementStatus failedStatus = new K8sPipelineElementStatus("Job/job1", false, true, "Failed.");
    when(elementStatusEstimator.estimateStatuses(any())).thenReturn(Collections.singletonList(failedStatus));

    Result result = reconciler.reconcile(new Request("ns", "failed-pipeline"));
    assertTrue(result.isRequeue());
    assertFalse(pipeline.getStatus().getReady());
    assertTrue(pipeline.getStatus().getFailed());
    assertEquals("Failed.", pipeline.getStatus().getMessage());
  }

  @Test
  void reconcileDeployedButNotReadyPipelineRequeues() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("pending-pipeline").namespace("ns"))
        .spec(new V1alpha1PipelineSpec().yaml("kind: Job\nmetadata:\n  name: job1"))
        .status(new V1alpha1PipelineStatus());
    pipelines.add(pipeline);

    K8sPipelineElementStatus pendingStatus = new K8sPipelineElementStatus("Job/job1", false, false, "Deployed.");
    when(elementStatusEstimator.estimateStatuses(any())).thenReturn(Collections.singletonList(pendingStatus));

    Result result = reconciler.reconcile(new Request("ns", "pending-pipeline"));
    assertTrue(result.isRequeue());
    assertFalse(pipeline.getStatus().getReady());
    assertFalse(pipeline.getStatus().getFailed());
    assertEquals("Deployed.", pipeline.getStatus().getMessage());
  }

  @Test
  void reconcileWithNullStatusInitializesStatus() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("no-status-pipeline").namespace("ns"))
        .spec(new V1alpha1PipelineSpec().yaml("kind: Job\nmetadata:\n  name: job1"));
    pipelines.add(pipeline);

    K8sPipelineElementStatus readyStatus = new K8sPipelineElementStatus("Job/job1", true, false, "Ready.");
    when(elementStatusEstimator.estimateStatuses(any())).thenReturn(Collections.singletonList(readyStatus));

    Result result = reconciler.reconcile(new Request("ns", "no-status-pipeline"));
    assertFalse(result.isRequeue());
    assertTrue(pipeline.getStatus().getReady());
  }

  @Test
  void reconcileWithEstimatorExceptionRequeuesWithFailureRetry() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("error-pipeline").namespace("ns"))
        .spec(new V1alpha1PipelineSpec().yaml("kind: Job\nmetadata:\n  name: job1"))
        .status(new V1alpha1PipelineStatus());
    pipelines.add(pipeline);

    when(elementStatusEstimator.estimateStatuses(any())).thenThrow(new RuntimeException("test error"));

    Result result = reconciler.reconcile(new Request("ns", "error-pipeline"));
    assertTrue(result.isRequeue());
  }

  @Test
  void reconcileWithMultipleElementsAllReadyDoesNotRequeue() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("multi-pipeline").namespace("ns"))
        .spec(new V1alpha1PipelineSpec().yaml("kind: Job\nmetadata:\n  name: job1\n---\nkind: Service\nmetadata:\n  name: svc1"));
    pipelines.add(pipeline);

    List<K8sPipelineElementStatus> statuses = List.of(
        new K8sPipelineElementStatus("Job/job1", true, false, "Ready."),
        new K8sPipelineElementStatus("Service/svc1", true, false, "Ready."));
    when(elementStatusEstimator.estimateStatuses(any())).thenReturn(statuses);

    Result result = reconciler.reconcile(new Request("ns", "multi-pipeline"));
    assertFalse(result.isRequeue());
    assertTrue(pipeline.getStatus().getReady());
  }

  @Test
  void reconcileWithMixedStatusesRequeues() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .metadata(new V1ObjectMeta().name("mixed-pipeline").namespace("ns"))
        .spec(new V1alpha1PipelineSpec().yaml("kind: Job\nmetadata:\n  name: job1"));
    pipelines.add(pipeline);

    List<K8sPipelineElementStatus> statuses = List.of(
        new K8sPipelineElementStatus("Job/job1", true, false, "Ready."),
        new K8sPipelineElementStatus("Service/svc1", false, false, "Deployed."));
    when(elementStatusEstimator.estimateStatuses(any())).thenReturn(statuses);

    Result result = reconciler.reconcile(new Request("ns", "mixed-pipeline"));
    assertTrue(result.isRequeue());
    assertFalse(pipeline.getStatus().getReady());
  }

  @Test
  @SuppressWarnings("unchecked")
  void controllerCreatesControllerFromContext() {
    SharedInformerFactory mockInformerFactory = mock(SharedInformerFactory.class);
    SharedIndexInformer<V1alpha1Pipeline> mockInformer = mock(SharedIndexInformer.class);
    when(mockInformerFactory.getExistingSharedIndexInformer(V1alpha1Pipeline.class)).thenReturn(mockInformer);
    K8sContext mockContext = mock(K8sContext.class);
    when(mockContext.informerFactory()).thenReturn(mockInformerFactory);

    Controller controller = PipelineReconciler.controller(mockContext);

    assertNotNull(controller);
  }
}
