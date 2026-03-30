package com.linkedin.hoptimator.operator.subscription;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.linkedin.hoptimator.catalog.Database;
import com.linkedin.hoptimator.catalog.HopTable;
import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.k8s.models.V1alpha1Subscription;
import com.linkedin.hoptimator.k8s.models.V1alpha1SubscriptionSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1SubscriptionStatus;
import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.planner.HoptimatorPlanner;
import com.linkedin.hoptimator.planner.PipelineRel;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class SubscriptionReconcilerTest {

  @Mock
  private Operator operator;

  @Mock
  private HoptimatorPlanner.Factory plannerFactory;

  @Mock
  private HoptimatorPlanner mockPlanner;

  @Mock
  private PipelineRel mockPlan;

  @Mock
  private Database mockDatabase;

  @Mock
  private GenericKubernetesApi<V1alpha1Subscription, ?> mockSubscriptionApi;

  @Mock
  private KubernetesApiResponse<V1alpha1Subscription> mockUpdateStatusResponse;

  private SubscriptionReconciler reconciler;

  private static SubscriptionReconciler createReconciler(Operator operator,
      HoptimatorPlanner.Factory plannerFactory,
      Resource.Environment environment,
      Predicate<V1alpha1Subscription> filter) {
    return new SubscriptionReconciler(operator, plannerFactory, environment, filter);
  }

  private RelDataType buildSimpleRowType() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    return typeFactory.builder().add("col", SqlTypeName.VARCHAR).build();
  }

  private V1alpha1Subscription buildSubscription(String ns, String name, String sql) {
    V1alpha1Subscription sub = new V1alpha1Subscription();
    sub.setMetadata(new V1ObjectMeta().name(name).namespace(ns));
    sub.setSpec(new V1alpha1SubscriptionSpec().sql(sql).database("TESTDB"));
    return sub;
  }

  @BeforeEach
  void setUp() {
    lenient().when(operator.failureRetryDuration()).thenReturn(Duration.ofMinutes(5));
    lenient().when(operator.pendingRetryDuration()).thenReturn(Duration.ofMinutes(1));
    reconciler = createReconciler(operator, plannerFactory, Resource.Environment.EMPTY, null);
  }

  // ── Helper: stub the apiFor(SUBSCRIPTION).updateStatus(...).onFailure(...) chain
  @SuppressWarnings("unchecked")
  private void stubUpdateStatus() throws ApiException {
    when(operator.apiFor(anyString())).thenReturn((GenericKubernetesApi) mockSubscriptionApi);
    when(mockSubscriptionApi.updateStatus(any(), any())).thenReturn(mockUpdateStatusResponse);
    when(mockUpdateStatusResponse.onFailure(any())).thenReturn(mockUpdateStatusResponse);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 1. Deleted object
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void deletedObjectDoesNotRequeue() {
    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(null);
    Result result = reconciler.reconcile(new Request("ns", "missing-sub"));
    assertFalse(result.isRequeue());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 2. Filtered object
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void filteredObjectDoesNotRequeue() {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    SubscriptionReconciler filteredReconciler =
        createReconciler(operator, plannerFactory, Resource.Environment.EMPTY, s -> false);
    Result result = filteredReconciler.reconcile(new Request("ns", "my-sub"));
    assertFalse(result.isRequeue());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 3. Phase 1 – planning error marks subscription failed
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void planningErrorMarksFailed() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(plannerFactory.makePlanner()).thenThrow(new RuntimeException("planner unavailable"));
    stubUpdateStatus();

    Result result = reconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(result.isRequeue());
    assertTrue(sub.getStatus().getFailed());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 4. Phase 1 – diverged: plannerFactory.makePlanner() is called when sql==null
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void phase1AttemptsPlanningWhenDiverged() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);

    // Planner returns a plan; Implementor will attempt SQL generation and fail
    // (mock RelNode lacks a Calcite cluster). The catch block captures this.
    when(plannerFactory.makePlanner()).thenReturn(mockPlanner);
    when(mockPlanner.pipeline(anyString())).thenReturn(mockPlan);
    when(mockPlan.getInputs()).thenReturn(Collections.emptyList());
    RelDataType rowType = buildSimpleRowType();
    when(mockPlan.getRowType()).thenReturn(rowType);
    when(mockPlanner.database(anyString())).thenReturn(mockDatabase);
    HopTable mockSink = new HopTable("TESTDB", "my-sub", rowType,
        Collections.emptyList(), Collections.emptyList(), new HashMap<>());
    when(mockDatabase.makeTable(anyString(), any())).thenReturn(mockSink);

    stubUpdateStatus();

    reconciler.reconcile(new Request("ns", "my-sub"));

    // plannerFactory.makePlanner() must have been called (phase 1 was entered)
    verify(plannerFactory).makePlanner();
    // status.failed is set when planning throws (SQL generation needs a real cluster)
    assertNotNull(sub.getStatus().getFailed());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 5. Phase 2 – deploy: ready==null and resources present → apply resources
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void phase2DeploysResourcesWhenReadyIsNull() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(null);
    status.setResources(Collections.singletonList(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: ns\n"));
    status.setJobResources(Collections.emptyList());
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    stubUpdateStatus();

    reconciler.reconcile(new Request("ns", "my-sub"));

    assertFalse(sub.getStatus().getReady());
    assertFalse(sub.getStatus().getFailed());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 6. Phase 3 – all resources ready → ready=true, no requeue
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void phase3ReadyWhenAllResourcesReady() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: ns\n";
    status.setResources(Collections.singletonList(yaml));
    status.setJobResources(Collections.emptyList());
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(operator.isReady(anyString())).thenReturn(true);
    stubUpdateStatus();

    Result result = reconciler.reconcile(new Request("ns", "my-sub"));

    assertFalse(result.isRequeue());
    assertTrue(sub.getStatus().getReady());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 7. Phase 3 – resource not ready → requeue
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void phase3RequeuedWhenResourceNotReady() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: ns\n";
    status.setResources(Collections.singletonList(yaml));
    status.setJobResources(Collections.emptyList());
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(operator.isReady(anyString())).thenReturn(false);
    stubUpdateStatus();

    Result result = reconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(result.isRequeue());
    assertFalse(sub.getStatus().getReady());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 8a. diverged() – sql null in status triggers phase 1
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void divergedWhenStatusSqlIsNull() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(plannerFactory.makePlanner()).thenThrow(new RuntimeException("expected diverged path"));
    stubUpdateStatus();

    reconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(sub.getStatus().getFailed());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 8b. diverged() – sql and hints match → not diverged
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void notDivergedWhenSqlAndHintsMatch() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(true);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: ns\n";
    status.setResources(Collections.singletonList(yaml));
    status.setJobResources(Collections.emptyList());
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);
    sub.getSpec().setHints(new HashMap<>());

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(operator.isReady(anyString())).thenReturn(true);
    stubUpdateStatus();

    Result result = reconciler.reconcile(new Request("ns", "my-sub"));

    assertFalse(result.isRequeue());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 8c. diverged() – hints differ → diverged
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void divergedWhenHintsDiffer() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    sub.getSpec().setHints(Collections.singletonMap("key", "value"));
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(true);
    status.setResources(Collections.emptyList());
    status.setJobResources(Collections.emptyList());
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(plannerFactory.makePlanner()).thenThrow(new RuntimeException("diverged path hit"));
    stubUpdateStatus();

    reconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(sub.getStatus().getFailed());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 9a. guessAttributes() – .status.attributes field
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void fetchAttributesFromStatusAttributesField() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);

    JsonObject attributes = new JsonObject();
    attributes.add("topicName", new JsonPrimitive("my-topic"));
    JsonObject statusObj = new JsonObject();
    statusObj.add("attributes", attributes);
    statusObj.add("ready", new JsonPrimitive(false));
    JsonObject raw = new JsonObject();
    raw.add("status", statusObj);
    raw.add("apiVersion", new JsonPrimitive("v1"));
    raw.add("kind", new JsonPrimitive("ConfigMap"));
    JsonObject metadata = new JsonObject();
    metadata.add("name", new JsonPrimitive("cm1"));
    metadata.add("namespace", new JsonPrimitive("ns"));
    raw.add("metadata", metadata);

    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: ns\nstatus:\n  attributes:\n    topicName: my-topic\n  ready: false\n";
    status.setResources(Collections.singletonList(yaml));
    status.setJobResources(Collections.singletonList(yaml));
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(operator.isReady(anyString())).thenReturn(false);

    DynamicKubernetesApi mockDynApi = mock(DynamicKubernetesApi.class);
    when(operator.apiFor(any(DynamicKubernetesObject.class))).thenReturn(mockDynApi);
    KubernetesApiResponse<DynamicKubernetesObject> mockResp = mock(KubernetesApiResponse.class);
    when(mockDynApi.get(anyString(), anyString())).thenReturn(mockResp);
    when(mockResp.onFailure(any())).thenReturn(mockResp);
    when(mockResp.isSuccess()).thenReturn(true);
    DynamicKubernetesObject dynObj = new DynamicKubernetesObject(raw);
    dynObj.setMetadata(new V1ObjectMeta().name("cm1").namespace("ns"));
    when(mockResp.getObject()).thenReturn(dynObj);

    stubUpdateStatus();

    reconciler.reconcile(new Request("ns", "my-sub"));

    assertNotNull(sub.getStatus().getAttributes());
    assertTrue(sub.getStatus().getAttributes().containsKey("topicName"));
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 9b. guessAttributes() – .status.jobStatus field
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void fetchAttributesFromStatusJobStatusField() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);

    JsonObject jobStatus = new JsonObject();
    jobStatus.add("jobId", new JsonPrimitive("job-123"));
    JsonObject statusObj = new JsonObject();
    statusObj.add("jobStatus", jobStatus);
    JsonObject raw = new JsonObject();
    raw.add("status", statusObj);
    raw.add("apiVersion", new JsonPrimitive("v1"));
    raw.add("kind", new JsonPrimitive("FlinkDeployment"));
    JsonObject metadata = new JsonObject();
    metadata.add("name", new JsonPrimitive("flink-job"));
    metadata.add("namespace", new JsonPrimitive("ns"));
    raw.add("metadata", metadata);

    String yaml = "apiVersion: v1\nkind: FlinkDeployment\nmetadata:\n  name: flink-job\n  namespace: ns\n";
    status.setResources(Collections.singletonList(yaml));
    status.setJobResources(Collections.singletonList(yaml));
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(operator.isReady(anyString())).thenReturn(false);

    DynamicKubernetesApi mockDynApi = mock(DynamicKubernetesApi.class);
    when(operator.apiFor(any(DynamicKubernetesObject.class))).thenReturn(mockDynApi);
    KubernetesApiResponse<DynamicKubernetesObject> mockResp = mock(KubernetesApiResponse.class);
    when(mockDynApi.get(anyString(), anyString())).thenReturn(mockResp);
    when(mockResp.onFailure(any())).thenReturn(mockResp);
    when(mockResp.isSuccess()).thenReturn(true);
    DynamicKubernetesObject dynObj = new DynamicKubernetesObject(raw);
    dynObj.setMetadata(new V1ObjectMeta().name("flink-job").namespace("ns"));
    when(mockResp.getObject()).thenReturn(dynObj);

    stubUpdateStatus();

    reconciler.reconcile(new Request("ns", "my-sub"));

    assertNotNull(sub.getStatus().getAttributes());
    assertTrue(sub.getStatus().getAttributes().containsKey("jobId"));
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 9c. fetchAttributes() – API call fails → empty attributes
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void fetchAttributesReturnsEmptyWhenApiFails() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);

    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: ns\n";
    status.setResources(Collections.singletonList(yaml));
    status.setJobResources(Collections.singletonList(yaml));
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(operator.isReady(anyString())).thenReturn(false);

    DynamicKubernetesApi mockDynApi = mock(DynamicKubernetesApi.class);
    when(operator.apiFor(any(DynamicKubernetesObject.class))).thenReturn(mockDynApi);
    KubernetesApiResponse<DynamicKubernetesObject> mockResp = mock(KubernetesApiResponse.class);
    when(mockDynApi.get(anyString(), anyString())).thenReturn(mockResp);
    when(mockResp.onFailure(any())).thenReturn(mockResp);
    when(mockResp.isSuccess()).thenReturn(false);

    stubUpdateStatus();

    reconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(sub.getStatus().getAttributes() == null
        || sub.getStatus().getAttributes().isEmpty());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 10. controller() – returns non-null Controller
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void controllerReturnsNonNull() {
    SharedInformerFactory mockInformerFactory = mock(SharedInformerFactory.class);
    SharedIndexInformer<V1alpha1Subscription> mockInformer = mock(SharedIndexInformer.class);
    when(mockInformerFactory.getExistingSharedIndexInformer(V1alpha1Subscription.class))
        .thenReturn(mockInformer);
    when(operator.informerFactory()).thenReturn(mockInformerFactory);

    Controller controller = SubscriptionReconciler.controller(operator, plannerFactory,
        Resource.Environment.EMPTY, null);
    assertNotNull(controller);
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 11. Phase 2 deploy failure (apply throws) → requeue
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void phase2RequeuedWhenApplyThrows() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(null);
    status.setResources(Collections.singletonList(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: ns\n"));
    status.setJobResources(Collections.emptyList());
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    doThrow(new ApiException("apply failed")).when(operator).apply(anyString(), any());

    Result result = reconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(result.isRequeue());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 12. Outer exception handler – fetch throws → requeue
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void outerExceptionHandlerRequeuedWhenFetchThrows() {
    when(operator.fetch(anyString(), anyString(), anyString()))
        .thenThrow(new RuntimeException("unexpected error"));

    Result result = reconciler.reconcile(new Request("ns", "boom"));

    assertTrue(result.isRequeue());
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 13. guessAttributes() – .status direct fields (no attributes/jobStatus)
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  @SuppressWarnings("unchecked")
  void fetchAttributesFromStatusDirectFields() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);

    // Build yaml where .status has primitive fields (no attributes/jobStatus keys)
    JsonObject statusObj = new JsonObject();
    statusObj.add("topicUrl", new JsonPrimitive("kafka://my-topic"));
    statusObj.add("ready", new JsonPrimitive(false));
    JsonObject raw = new JsonObject();
    raw.add("status", statusObj);
    raw.add("apiVersion", new JsonPrimitive("v1alpha1"));
    raw.add("kind", new JsonPrimitive("KafkaTopic"));
    JsonObject metadata = new JsonObject();
    metadata.add("name", new JsonPrimitive("topic1"));
    metadata.add("namespace", new JsonPrimitive("ns"));
    raw.add("metadata", metadata);

    String yaml = "apiVersion: v1alpha1\nkind: KafkaTopic\nmetadata:\n  name: topic1\n  namespace: ns\n";
    status.setResources(Collections.singletonList(yaml));
    status.setJobResources(Collections.singletonList(yaml));
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(operator.isReady(anyString())).thenReturn(false);

    DynamicKubernetesApi mockDynApi = mock(DynamicKubernetesApi.class);
    when(operator.apiFor(any(DynamicKubernetesObject.class))).thenReturn(mockDynApi);
    KubernetesApiResponse<DynamicKubernetesObject> mockResp = mock(KubernetesApiResponse.class);
    when(mockDynApi.get(anyString(), anyString())).thenReturn(mockResp);
    when(mockResp.onFailure(any())).thenReturn(mockResp);
    when(mockResp.isSuccess()).thenReturn(true);
    DynamicKubernetesObject dynObj = new DynamicKubernetesObject(raw);
    dynObj.setMetadata(new V1ObjectMeta().name("topic1").namespace("ns"));
    when(mockResp.getObject()).thenReturn(dynObj);

    stubUpdateStatus();

    reconciler.reconcile(new Request("ns", "my-sub"));

    // .status has topicUrl and ready as primitives → fetched as attributes
    assertNotNull(sub.getStatus().getAttributes());
    assertTrue(sub.getStatus().getAttributes().containsKey("topicUrl"));
  }

  // ─────────────────────────────────────────────────────────────────────────────
  // 14. fetchAttributes() – operator.apiFor(obj) throws → returns empty
  // ─────────────────────────────────────────────────────────────────────────────

  @Test
  void fetchAttributesReturnsEmptyWhenApiForThrows() throws Exception {
    V1alpha1Subscription sub = buildSubscription("ns", "my-sub", "SELECT 1");
    V1alpha1SubscriptionStatus status = new V1alpha1SubscriptionStatus();
    status.setSql("SELECT 1");
    status.setHints(new HashMap<>());
    status.setReady(false);

    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cm1\n  namespace: ns\n";
    status.setResources(Collections.singletonList(yaml));
    status.setJobResources(Collections.singletonList(yaml));
    status.setDownstreamResources(Collections.emptyList());
    sub.setStatus(status);

    when(operator.fetch(anyString(), anyString(), anyString())).thenReturn(sub);
    when(operator.isReady(anyString())).thenReturn(false);
    when(operator.apiFor(any(DynamicKubernetesObject.class)))
        .thenThrow(new RuntimeException("no api registered"));

    stubUpdateStatus();

    // Should not throw; attributes will be empty
    reconciler.reconcile(new Request("ns", "my-sub"));

    assertTrue(sub.getStatus().getAttributes() == null
        || sub.getStatus().getAttributes().isEmpty());
  }
}
