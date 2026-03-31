package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.options.DeleteOptions;
import io.kubernetes.client.util.generic.options.ListOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"},
    justification = "Mocked AutoCloseable and return values not needed in tests")
@ExtendWith(MockitoExtension.class)
class K8sApiTest {

  @Mock
  private K8sContext mockContext;

  @Mock
  private GenericKubernetesApi<V1alpha1Pipeline, V1alpha1PipelineList> mockGenericApi;

  @Mock
  private KubernetesApiResponse<V1alpha1Pipeline> mockSingleResponse;

  @Mock
  private KubernetesApiResponse<V1alpha1PipelineList> mockListResponse;

  private K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> api;
  private K8sApiEndpoint<V1alpha1Pipeline, V1alpha1PipelineList> endpoint;

  @BeforeEach
  void setUp() {
    endpoint = K8sApiEndpoints.PIPELINES;
    api = new K8sApi<>(mockContext, endpoint);
    lenient().when(mockContext.generic(endpoint)).thenReturn(mockGenericApi);
    lenient().when(mockContext.namespace()).thenReturn("test-ns");
  }

  @Test
  void endpointReturnsTheEndpoint() {
    assertEquals(endpoint, api.endpoint());
  }

  @Test
  void listDelegatesToSelect() throws SQLException {
    V1alpha1PipelineList pipelineList = new V1alpha1PipelineList();
    V1alpha1Pipeline pipeline = makePipeline("test-pipeline", "test-ns");
    pipelineList.setItems(Collections.singletonList(pipeline));

    when(mockGenericApi.list(eq("test-ns"), any(ListOptions.class))).thenReturn(mockListResponse);
    when(mockListResponse.getObject()).thenReturn(pipelineList);

    Collection<V1alpha1Pipeline> result = api.list();

    assertEquals(1, result.size());
  }

  @Test
  void listReturnsEmptyCollectionOn404() throws SQLException {
    when(mockGenericApi.list(eq("test-ns"), any(ListOptions.class))).thenReturn(mockListResponse);
    when(mockListResponse.getHttpStatusCode()).thenReturn(404);

    Collection<V1alpha1Pipeline> result = api.list();

    assertEquals(0, result.size());
  }

  @Test
  void getByNameDelegatesToNamespacedGet() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("my-pipeline", "test-ns");

    when(mockGenericApi.get(eq("test-ns"), eq("my-pipeline"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.getObject()).thenReturn(pipeline);

    V1alpha1Pipeline result = api.get("my-pipeline");

    assertEquals("my-pipeline", result.getMetadata().getName());
  }

  @Test
  void getByNameAndNamespaceDelegatesToGenericApi() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("my-pipeline", "other-ns");

    when(mockGenericApi.get(eq("other-ns"), eq("my-pipeline"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.getObject()).thenReturn(pipeline);

    V1alpha1Pipeline result = api.get("other-ns", "my-pipeline");

    assertEquals("my-pipeline", result.getMetadata().getName());
  }

  @Test
  void getByObjectWithExplicitNamespace() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("my-pipeline", "explicit-ns");

    when(mockGenericApi.get(eq("explicit-ns"), eq("my-pipeline"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.getObject()).thenReturn(pipeline);

    V1alpha1Pipeline result = api.get(pipeline);

    assertEquals("my-pipeline", result.getMetadata().getName());
  }

  @Test
  void getByObjectSetsContextNamespaceWhenNull() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("my-pipeline", null);

    when(mockGenericApi.get(eq("test-ns"), eq("my-pipeline"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.getObject()).thenReturn(pipeline);

    V1alpha1Pipeline result = api.get(pipeline);

    assertEquals("my-pipeline", result.getMetadata().getName());
  }

  @Test
  void getIfExistsReturnsNullOn404() throws SQLException {
    when(mockGenericApi.get(eq("test-ns"), eq("missing"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.getHttpStatusCode()).thenReturn(404);

    V1alpha1Pipeline result = api.getIfExists("test-ns", "missing");

    assertNull(result);
  }

  @Test
  void getIfExistsReturnsObjectOnSuccess() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("my-pipeline", "test-ns");

    when(mockGenericApi.get(eq("test-ns"), eq("my-pipeline"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.getObject()).thenReturn(pipeline);

    V1alpha1Pipeline result = api.getIfExists("test-ns", "my-pipeline");

    assertNotNull(result);
    assertEquals("my-pipeline", result.getMetadata().getName());
  }

  @Test
  void createSetsNamespaceAndCallsOwnAndCreate() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("new-pipeline", null);

    when(mockGenericApi.create(any(V1alpha1Pipeline.class))).thenReturn(mockSingleResponse);

    api.create(pipeline);

    verify(mockContext).own(pipeline);
    verify(mockGenericApi).create(pipeline);
    assertEquals("test-ns", pipeline.getMetadata().getNamespace());
  }

  @Test
  void createWithExplicitNamespaceCallsOwnAndCreate() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("new-pipeline", "explicit-ns");

    when(mockGenericApi.create(any(V1alpha1Pipeline.class))).thenReturn(mockSingleResponse);

    api.create(pipeline);

    verify(mockContext).own(pipeline);
    verify(mockGenericApi).create(pipeline);
  }

  @Test
  void deleteByObjectDelegatesToDeleteNamespaceName() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("to-delete", "test-ns");

    when(mockGenericApi.delete(eq("test-ns"), eq("to-delete"), any(DeleteOptions.class)))
        .thenReturn(mockSingleResponse);

    api.delete(pipeline);

    verify(mockGenericApi).delete(eq("test-ns"), eq("to-delete"), any(DeleteOptions.class));
  }

  @Test
  void deleteByObjectSetsContextNamespaceWhenNull() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("to-delete", null);

    when(mockGenericApi.delete(eq("test-ns"), eq("to-delete"), any(DeleteOptions.class)))
        .thenReturn(mockSingleResponse);

    api.delete(pipeline);

    verify(mockGenericApi).delete(eq("test-ns"), eq("to-delete"), any(DeleteOptions.class));
  }

  @Test
  void deleteByNamespaceAndNameCallsGenericApi() throws SQLException {
    when(mockGenericApi.delete(eq("ns"), eq("name"), any(DeleteOptions.class)))
        .thenReturn(mockSingleResponse);

    api.delete("ns", "name");

    verify(mockGenericApi).delete(eq("ns"), eq("name"), any(DeleteOptions.class));
  }

  @Test
  void deleteByNameUsesContextNamespace() throws SQLException {
    when(mockGenericApi.delete(eq("test-ns"), eq("name"), any(DeleteOptions.class)))
        .thenReturn(mockSingleResponse);

    api.delete("name");

    verify(mockGenericApi).delete(eq("test-ns"), eq("name"), any(DeleteOptions.class));
  }

  @Test
  void updateWhenObjectExistsCallsUpdate() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("existing", "test-ns");
    V1alpha1Pipeline existing = makePipeline("existing", "test-ns");
    existing.getMetadata().setResourceVersion("rv1");

    when(mockGenericApi.get(eq("test-ns"), eq("existing"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.isSuccess()).thenReturn(true);
    when(mockSingleResponse.getObject()).thenReturn(existing);
    when(mockGenericApi.update(any(V1alpha1Pipeline.class))).thenReturn(mockSingleResponse);

    api.update(pipeline);

    verify(mockGenericApi).update(pipeline);
    verify(mockContext).own(pipeline);
    assertEquals("rv1", pipeline.getMetadata().getResourceVersion());
  }

  @Test
  void updateMergesLabelsFromExisting() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("existing", "test-ns");
    Map<String, String> newLabels = new HashMap<>();
    newLabels.put("new-key", "new-val");
    pipeline.getMetadata().setLabels(newLabels);

    V1alpha1Pipeline existing = makePipeline("existing", "test-ns");
    Map<String, String> existingLabels = new HashMap<>();
    existingLabels.put("existing-key", "existing-val");
    existing.getMetadata().setLabels(existingLabels);
    existing.getMetadata().setResourceVersion("rv2");

    when(mockGenericApi.get(eq("test-ns"), eq("existing"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.isSuccess()).thenReturn(true);
    when(mockSingleResponse.getObject()).thenReturn(existing);
    when(mockGenericApi.update(any(V1alpha1Pipeline.class))).thenReturn(mockSingleResponse);

    api.update(pipeline);

    Map<String, String> mergedLabels = pipeline.getMetadata().getLabels();
    assertEquals("existing-val", mergedLabels.get("existing-key"));
    assertEquals("new-val", mergedLabels.get("new-key"));
  }

  @Test
  void updateWhenObjectNotExistsCallsCreate() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("new-pipeline", "test-ns");

    when(mockGenericApi.get(eq("test-ns"), eq("new-pipeline"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.isSuccess()).thenReturn(false);
    when(mockGenericApi.create(any(V1alpha1Pipeline.class))).thenReturn(mockSingleResponse);

    api.update(pipeline);

    verify(mockGenericApi).create(pipeline);
    verify(mockContext).own(pipeline);
  }

  @Test
  void updateSetsNamespaceFromContextWhenNull() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("new-pipeline", null);

    when(mockGenericApi.get(eq("test-ns"), eq("new-pipeline"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.isSuccess()).thenReturn(false);
    when(mockGenericApi.create(any(V1alpha1Pipeline.class))).thenReturn(mockSingleResponse);

    api.update(pipeline);

    assertEquals("test-ns", pipeline.getMetadata().getNamespace());
  }

  @Test
  void updateStatusDelegatesToGenericUpdateStatus() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("my-pipeline", "test-ns");
    Object status = new Object();

    when(mockGenericApi.updateStatus(any(V1alpha1Pipeline.class), any())).thenReturn(mockSingleResponse);

    api.updateStatus(pipeline, status);

    verify(mockGenericApi).updateStatus(eq(pipeline), any());
  }

  @Test
  void updateStatusSetsNamespaceWhenNull() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("my-pipeline", null);
    Object status = new Object();

    when(mockGenericApi.updateStatus(any(V1alpha1Pipeline.class), any())).thenReturn(mockSingleResponse);

    api.updateStatus(pipeline, status);

    assertEquals("test-ns", pipeline.getMetadata().getNamespace());
  }

  @Test
  void selectWithLabelSelectorPassesLabelSelectorToGenericApi() throws SQLException {
    V1alpha1PipelineList pipelineList = new V1alpha1PipelineList();
    pipelineList.setItems(Collections.emptyList());

    ArgumentCaptor<ListOptions> optionsCaptor = ArgumentCaptor.forClass(ListOptions.class);
    when(mockGenericApi.list(eq("test-ns"), optionsCaptor.capture())).thenReturn(mockListResponse);
    when(mockListResponse.getObject()).thenReturn(pipelineList);

    api.select("app=my-app");

    assertEquals("app=my-app", optionsCaptor.getValue().getLabelSelector());
  }

  @Test
  void selectWithNullLabelSelectorPassesNullLabelSelector() throws SQLException {
    V1alpha1PipelineList pipelineList = new V1alpha1PipelineList();
    pipelineList.setItems(Collections.emptyList());

    ArgumentCaptor<ListOptions> optionsCaptor = ArgumentCaptor.forClass(ListOptions.class);
    when(mockGenericApi.list(eq("test-ns"), optionsCaptor.capture())).thenReturn(mockListResponse);
    when(mockListResponse.getObject()).thenReturn(pipelineList);

    api.select(null);

    assertNull(optionsCaptor.getValue().getLabelSelector());
  }

  @Test
  void selectReturnsAllItemsFromResponse() throws SQLException {
    V1alpha1Pipeline p1 = makePipeline("p1", "test-ns");
    V1alpha1Pipeline p2 = makePipeline("p2", "test-ns");
    V1alpha1PipelineList pipelineList = new V1alpha1PipelineList();
    pipelineList.setItems(Arrays.asList(p1, p2));

    when(mockGenericApi.list(eq("test-ns"), any(ListOptions.class))).thenReturn(mockListResponse);
    when(mockListResponse.getObject()).thenReturn(pipelineList);

    Collection<V1alpha1Pipeline> result = api.select(null);

    assertEquals(2, result.size());
  }

  @Test
  void referenceReturnsOwnerReference() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("ref-pipeline", "test-ns");
    pipeline.setKind("Pipeline");
    pipeline.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    pipeline.getMetadata().setUid("uid-123");

    when(mockGenericApi.get(eq("test-ns"), eq("ref-pipeline"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.getObject()).thenReturn(pipeline);

    V1OwnerReference ref = api.reference(pipeline);

    assertEquals("Pipeline", ref.getKind());
    assertEquals("ref-pipeline", ref.getName());
    assertEquals("uid-123", ref.getUid());
  }

  @Test
  void updateMergesOwnerReferencesFromExisting() throws SQLException {
    V1alpha1Pipeline pipeline = makePipeline("existing", "test-ns");
    V1OwnerReference newOwner = new V1OwnerReference().name("new-owner").uid("uid-new");
    pipeline.getMetadata().setOwnerReferences(Collections.singletonList(newOwner));

    V1alpha1Pipeline existing = makePipeline("existing", "test-ns");
    V1OwnerReference existingOwner = new V1OwnerReference().name("existing-owner").uid("uid-existing");
    existing.getMetadata().setOwnerReferences(Collections.singletonList(existingOwner));
    existing.getMetadata().setResourceVersion("rv3");

    when(mockGenericApi.get(eq("test-ns"), eq("existing"))).thenReturn(mockSingleResponse);
    when(mockSingleResponse.isSuccess()).thenReturn(true);
    when(mockSingleResponse.getObject()).thenReturn(existing);
    when(mockGenericApi.update(any(V1alpha1Pipeline.class))).thenReturn(mockSingleResponse);

    api.update(pipeline);

    List<V1OwnerReference> owners = pipeline.getMetadata().getOwnerReferences();
    assertEquals(2, owners.size());
  }

  // ── Cluster-scoped endpoint tests (NAMESPACES is cluster-scoped) ──

  @Nested
  @ExtendWith(MockitoExtension.class)
  @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"},
      justification = "Mocked AutoCloseable and return values not needed in tests")
  class ClusterScopedEndpointTests {

    @Mock
    private K8sContext mockClusterContext;

    @Mock
    private GenericKubernetesApi<V1Namespace, V1NamespaceList> mockNsApi;

    @Mock
    private KubernetesApiResponse<V1Namespace> mockNsResponse;

    @Mock
    private KubernetesApiResponse<V1NamespaceList> mockNsListResponse;

    private K8sApi<V1Namespace, V1NamespaceList> nsApi;

    @BeforeEach
    void setUp() {
      nsApi = new K8sApi<>(mockClusterContext, K8sApiEndpoints.NAMESPACES);
      lenient().when(mockClusterContext.generic(K8sApiEndpoints.NAMESPACES)).thenReturn(mockNsApi);
      lenient().when(mockClusterContext.namespace()).thenReturn("test-ns");
    }

    private V1Namespace makeNamespace(String name) {
      V1Namespace ns = new V1Namespace();
      ns.setMetadata(new V1ObjectMeta().name(name));
      return ns;
    }

    @Test
    void getByNameUsesNameOnlyVariantForClusterScoped() throws SQLException {
      V1Namespace ns = makeNamespace("my-ns");
      when(mockNsApi.get(eq("my-ns"))).thenReturn(mockNsResponse);
      when(mockNsResponse.getObject()).thenReturn(ns);

      V1Namespace result = nsApi.get("my-ns");

      verify(mockNsApi, times(1)).get(eq("my-ns"));
      verify(mockNsApi, never()).get(anyNsNamespace(), eq("my-ns"));
      assertEquals("my-ns", result.getMetadata().getName());
    }

    @Test
    void getByObjectUsesNameOnlyForClusterScopedWhenNamespaceIsNull() throws SQLException {
      V1Namespace ns = makeNamespace("my-ns");
      when(mockNsApi.get(eq("my-ns"))).thenReturn(mockNsResponse);
      when(mockNsResponse.getObject()).thenReturn(ns);

      nsApi.get(ns);

      verify(mockNsApi, times(1)).get(eq("my-ns"));
      verify(mockNsApi, never()).get(any(String.class), eq("my-ns"));
    }

    @Test
    void getByObjectDoesNotSetNamespaceForClusterScoped() throws SQLException {
      V1Namespace ns = makeNamespace("my-ns");
      // namespace is null on the object
      when(mockNsApi.get(eq("my-ns"))).thenReturn(mockNsResponse);
      when(mockNsResponse.getObject()).thenReturn(ns);

      nsApi.get(ns);

      // Namespace should NOT be set on a cluster-scoped object
      assertNull(ns.getMetadata().getNamespace());
    }

    @Test
    void createDoesNotSetNamespaceForClusterScoped() throws SQLException {
      V1Namespace ns = makeNamespace("my-ns");
      when(mockNsApi.create(any(V1Namespace.class))).thenReturn(mockNsResponse);

      nsApi.create(ns);

      verify(mockNsApi, times(1)).create(ns);
      assertNull(ns.getMetadata().getNamespace());
    }

    @Test
    void deleteDoesNotSetNamespaceForClusterScoped() throws SQLException {
      V1Namespace ns = makeNamespace("my-ns");
      when(mockNsApi.delete(any(), any(), any(DeleteOptions.class))).thenReturn(mockNsResponse);

      nsApi.delete(ns);

      // Should use null namespace (as-is), not context namespace
      assertNull(ns.getMetadata().getNamespace());
    }

    @Test
    void updateUsesNameOnlyGetForClusterScoped() throws SQLException {
      V1Namespace ns = makeNamespace("my-ns");
      V1Namespace existing = makeNamespace("my-ns");
      existing.getMetadata().setResourceVersion("rv-cs");

      when(mockNsApi.get(eq("my-ns"))).thenReturn(mockNsResponse);
      when(mockNsResponse.isSuccess()).thenReturn(true);
      when(mockNsResponse.getObject()).thenReturn(existing);
      when(mockNsApi.update(any(V1Namespace.class))).thenReturn(mockNsResponse);

      nsApi.update(ns);

      verify(mockNsApi, times(1)).get(eq("my-ns"));
      verify(mockNsApi, never()).get(any(String.class), eq("my-ns"));
      verify(mockNsApi, times(1)).update(ns);
    }

    @Test
    void updateStatusDoesNotSetNamespaceForClusterScoped() throws SQLException {
      V1Namespace ns = makeNamespace("my-ns");
      when(mockNsApi.updateStatus(any(V1Namespace.class), any())).thenReturn(mockNsResponse);

      nsApi.updateStatus(ns, new Object());

      assertNull(ns.getMetadata().getNamespace());
    }

    @Test
    void updateWhenObjectNotFoundForClusterScopedCallsCreate() throws SQLException {
      V1Namespace ns = makeNamespace("my-ns");

      when(mockNsApi.get(eq("my-ns"))).thenReturn(mockNsResponse);
      when(mockNsResponse.isSuccess()).thenReturn(false);
      when(mockNsApi.create(any(V1Namespace.class))).thenReturn(mockNsResponse);

      nsApi.update(ns);

      verify(mockNsApi, times(1)).create(ns);
      verify(mockNsApi, never()).update(any());
    }

    @Test
    void listUsesNameOnlyVariantForClusterScoped() throws SQLException {
      V1NamespaceList nsList = new V1NamespaceList();
      nsList.setItems(Collections.emptyList());
      when(mockNsApi.list(any(ListOptions.class))).thenReturn(mockNsListResponse);
      when(mockNsListResponse.getObject()).thenReturn(nsList);

      Collection<V1Namespace> result = nsApi.list();

      verify(mockNsApi, times(1)).list(any(ListOptions.class));
      verify(mockNsApi, never()).list(eq("test-ns"), any(ListOptions.class));
      assertEquals(0, result.size());
    }

    // Helper for never() verification — matches any string argument
    private String anyNsNamespace() {
      return any(String.class);
    }
  }

  private V1alpha1Pipeline makePipeline(String name, String namespace) {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline();
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(name);
    meta.setNamespace(namespace);
    pipeline.setMetadata(meta);
    return pipeline;
  }
}
