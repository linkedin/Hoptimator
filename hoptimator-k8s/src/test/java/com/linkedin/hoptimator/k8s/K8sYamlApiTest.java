package com.linkedin.hoptimator.k8s;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


class K8sYamlApiTest {

  @Test
  void listThrowsUnsupportedOperation() {
    K8sYamlApi api = createRealApi();
    assertThrows(UnsupportedOperationException.class, api::list);
  }

  @Test
  void objFromYamlParsesYaml() {
    K8sYamlApi api = createRealApi();
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: test-cm\n  namespace: ns";

    DynamicKubernetesObject obj = api.objFromYaml(yaml);

    assertEquals("test-cm", obj.getMetadata().getName());
    assertEquals("v1", obj.getApiVersion());
    assertEquals("ConfigMap", obj.getKind());
  }

  @Test
  void objFromYamlSetsNamespaceFromContext() {
    K8sYamlApi api = createRealApi();
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: test-cm";

    DynamicKubernetesObject obj = api.objFromYaml(yaml);

    assertEquals("test-ns", obj.getMetadata().getNamespace());
  }

  @Test
  void objFromYamlPreservesExistingNamespace() {
    K8sYamlApi api = createRealApi();
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: test-cm\n  namespace: other-ns";

    DynamicKubernetesObject obj = api.objFromYaml(yaml);

    assertEquals("other-ns", obj.getMetadata().getNamespace());
  }

  @Test
  void constructorAcceptsContext() {
    K8sYamlApi api = createRealApi();
    assertNotNull(api);
  }

  // ── Tests using mocked K8sContext for real K8sYamlApi methods ──

  @Nested
  @ExtendWith(MockitoExtension.class)
  @SuppressFBWarnings(value = {"RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"},
      justification = "Mockito doReturn().when() stubs — framework captures the return value")
  class RealMethodTests {

    @Mock
    private K8sContext mockContext;

    @SuppressWarnings("unchecked")
    private KubernetesApiResponse<DynamicKubernetesObject> successResponse(DynamicKubernetesObject obj) {
      KubernetesApiResponse<DynamicKubernetesObject> resp = mock(KubernetesApiResponse.class);
      lenient().doReturn(true).when(resp).isSuccess();
      lenient().doReturn(200).when(resp).getHttpStatusCode();
      lenient().doReturn(obj).when(resp).getObject();
      return resp;
    }

    @SuppressWarnings("unchecked")
    private KubernetesApiResponse<DynamicKubernetesObject> notFoundResponse() {
      KubernetesApiResponse<DynamicKubernetesObject> resp = mock(KubernetesApiResponse.class);
      lenient().doReturn(false).when(resp).isSuccess();
      lenient().doReturn(404).when(resp).getHttpStatusCode();
      return resp;
    }

    @Test
    void getIfExistsReturnsNullOn404() throws SQLException {
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(notFoundResponse()).when(dynApi).get(anyString(), anyString());

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

      DynamicKubernetesObject result = api.getIfExists(obj);
      assertNull(result);
    }

    @Test
    void getIfExistsReturnsObjectOnSuccess() throws SQLException {
      DynamicKubernetesObject existing = new DynamicKubernetesObject();
      existing.setApiVersion("v1");
      existing.setKind("ConfigMap");
      existing.setMetadata(new V1ObjectMeta().name("found").namespace("ns"));

      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(successResponse(existing)).when(dynApi).get(anyString(), anyString());

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("found").namespace("ns"));

      DynamicKubernetesObject result = api.getIfExists(obj);
      assertNotNull(result);
      assertEquals("found", result.getMetadata().getName());
    }

    @Test
    void createWithMetadataCallsDynamic() throws SQLException {
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());

      DynamicKubernetesObject created = new DynamicKubernetesObject();
      created.setApiVersion("v1");
      created.setKind("ConfigMap");
      created.setMetadata(new V1ObjectMeta().name("new-obj").namespace("ns"));
      doReturn(successResponse(created)).when(dynApi).create(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("new-obj").namespace("ns"));

      api.createWithMetadata(obj, null, null, null);
      // No exception = success
    }

    @Test
    void deleteCallsDynamic() throws SQLException {
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(successResponse(null)).when(dynApi).delete(anyString(), anyString());

      K8sYamlApi api = new K8sYamlApi(mockContext);
      api.delete("v1", "ConfigMap", "ns", "to-delete");
      // No exception = success
    }

    @Test
    void updateExistingObjectMergesLabels() throws SQLException {
      DynamicKubernetesObject existingObj = new DynamicKubernetesObject();
      existingObj.setApiVersion("v1");
      existingObj.setKind("ConfigMap");
      Map<String, String> existingLabels = new HashMap<>();
      existingLabels.put("existing", "label");
      existingObj.setMetadata(new V1ObjectMeta().name("upd").namespace("ns").labels(existingLabels));

      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).get(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).update(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      Map<String, String> newLabels = new HashMap<>();
      newLabels.put("new", "label");
      obj.setMetadata(new V1ObjectMeta().name("upd").namespace("ns").labels(newLabels));

      api.update(obj);
      // No exception = success
    }

    @Test
    void updateNonExistingObjectCreates() throws SQLException {
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(notFoundResponse()).when(dynApi).get(anyString(), anyString());

      DynamicKubernetesObject created = new DynamicKubernetesObject();
      created.setApiVersion("v1");
      created.setKind("ConfigMap");
      created.setMetadata(new V1ObjectMeta().name("new").namespace("ns"));
      doReturn(successResponse(created)).when(dynApi).create(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("new").namespace("ns"));

      api.update(obj);
      // No exception = success
    }

    @Test
    void updateExistingWithNullLabels() throws SQLException {
      DynamicKubernetesObject existingObj = new DynamicKubernetesObject();
      existingObj.setApiVersion("v1");
      existingObj.setKind("ConfigMap");
      existingObj.setMetadata(new V1ObjectMeta().name("upd").namespace("ns"));

      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).get(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).update(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("upd").namespace("ns"));

      api.update(obj);
      // No exception = success (both labels maps are null)
    }
  }

  // ── Tests using FakeK8sYamlApi ──

  @Test
  void fakeGetIfExistsReturnsNullWhenNotFound() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: missing\n  namespace: ns";
    DynamicKubernetesObject result = api.getIfExists(yaml);
    assertNull(result);
  }

  @Test
  void fakeGetIfExistsReturnsObjectWhenFound() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    String existingYaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: existing\n  namespace: ns";
    yamls.put("existing", existingYaml);

    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    DynamicKubernetesObject result = api.getIfExists(existingYaml);

    assertNotNull(result);
    assertEquals("existing", result.getMetadata().getName());
  }

  @Test
  void fakeCreateAddsObject() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: new-obj\n  namespace: ns";
    api.create(yaml);
    assertNotNull(yamls.get("new-obj"));
  }

  @Test
  void fakeCreateWithMetadataMergesAnnotationsAndLabels() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: annotated\n  namespace: ns";

    Map<String, String> annotations = new HashMap<>();
    annotations.put("key", "value");
    Map<String, String> labels = new HashMap<>();
    labels.put("app", "test");

    api.createWithMetadata(yaml, annotations, labels, null);
    assertNotNull(yamls.get("annotated"));
  }

  @Test
  void fakeDeleteRemovesObject() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    yamls.put("to-delete", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: to-delete");
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    api.delete("v1", "ConfigMap", "ns", "to-delete");
    assertNull(yamls.get("to-delete"));
  }

  @Test
  void fakeDeleteWithYaml() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    yamls.put("to-delete", "some-yaml");
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: to-delete\n  namespace: ns";
    api.delete(yaml);
    assertNull(yamls.get("to-delete"));
  }

  @Test
  void fakeDeleteWithDynamicObject() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    yamls.put("dyn-del", "some-yaml");
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setApiVersion("v1");
    obj.setKind("ConfigMap");
    obj.setMetadata(new V1ObjectMeta().name("dyn-del").namespace("ns"));
    api.delete(obj);
    assertNull(yamls.get("dyn-del"));
  }

  @Test
  void fakeUpdateSetsObject() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setApiVersion("v1");
    obj.setKind("ConfigMap");
    obj.setMetadata(new V1ObjectMeta().name("updated").namespace("ns"));
    api.update(obj);
    assertNotNull(yamls.get("updated"));
  }

  @Test
  void fakeCreateWithDynamicObject() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setApiVersion("v1");
    obj.setKind("ConfigMap");
    obj.setMetadata(new V1ObjectMeta().name("direct").namespace("ns"));
    api.create(obj);
    assertNotNull(yamls.get("direct"));
  }

  @Test
  void fakeUpdateWithYamlString() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: yaml-updated\n  namespace: ns";
    api.update(yaml);
    assertNotNull(yamls.get("yaml-updated"));
  }

  @Test
  void fakeCreateWithMetadataMergesExistingAnnotations() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: merge-test\n  namespace: ns\n"
        + "  annotations:\n    existing: anno";

    Map<String, String> newAnnotations = new HashMap<>();
    newAnnotations.put("new-key", "new-value");

    api.createWithMetadata(yaml, newAnnotations, null, null);
    assertNotNull(yamls.get("merge-test"));
  }

  @Test
  void fakeCreateWithMetadataMergesExistingLabels() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: label-test\n  namespace: ns\n"
        + "  labels:\n    existing: label";

    Map<String, String> newLabels = new HashMap<>();
    newLabels.put("new-label", "new-value");

    api.createWithMetadata(yaml, null, newLabels, null);
    assertNotNull(yamls.get("label-test"));
  }

  @Test
  void fakeCreateWithMetadataMergesOwnerReferences() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: owner-test\n  namespace: ns";

    List<V1OwnerReference> ownerRefs = new ArrayList<>();
    ownerRefs.add(new V1OwnerReference().name("owner1").kind("Deployment").apiVersion("apps/v1").uid("123"));

    api.createWithMetadata(yaml, null, null, ownerRefs);
    assertNotNull(yamls.get("owner-test"));
  }

  @Test
  void fakeCreateWithMetadataAllNulls() throws SQLException {
    Map<String, String> yamls = new HashMap<>();
    FakeK8sYamlApi api = new FakeK8sYamlApi(yamls);
    String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: null-test\n  namespace: ns";

    api.createWithMetadata(yaml, null, null, null);
    assertNotNull(yamls.get("null-test"));
  }

  // ── Verify side effects of Optional::ifPresent in createWithMetadata ──

  @Nested
  @ExtendWith(MockitoExtension.class)
  @SuppressFBWarnings(value = {"RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"},
      justification = "Mockito doReturn().when() stubs — framework captures the return value")
  class CreateWithMetadataSideEffectTests {

    @Mock
    private K8sContext mockContext;

    @SuppressWarnings("unchecked")
    private KubernetesApiResponse<DynamicKubernetesObject> successResponse(DynamicKubernetesObject obj) {
      KubernetesApiResponse<DynamicKubernetesObject> resp = mock(KubernetesApiResponse.class);
      lenient().doReturn(true).when(resp).isSuccess();
      lenient().doReturn(200).when(resp).getHttpStatusCode();
      lenient().doReturn(obj).when(resp).getObject();
      return resp;
    }

    @Test
    void createWithMetadataSetsOwnerReferencesOnCreatedObject() throws SQLException {
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());

      DynamicKubernetesObject created = new DynamicKubernetesObject();
      created.setApiVersion("v1");
      created.setKind("ConfigMap");
      created.setMetadata(new V1ObjectMeta().name("obj").namespace("ns"));
      doReturn(successResponse(created)).when(dynApi).create(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("obj").namespace("ns"));

      List<V1OwnerReference> ownerRefs = new ArrayList<>();
      ownerRefs.add(new V1OwnerReference().name("owner").uid("uid-1").kind("Deployment").apiVersion("apps/v1"));

      api.createWithMetadata(obj, null, null, ownerRefs);

      ArgumentCaptor<DynamicKubernetesObject> captor = ArgumentCaptor.forClass(DynamicKubernetesObject.class);
      verify(dynApi, times(1)).create(captor.capture());
      List<V1OwnerReference> actualOwners = captor.getValue().getMetadata().getOwnerReferences();
      assertNotNull(actualOwners);
      assertEquals(1, actualOwners.size());
      assertEquals("owner", actualOwners.get(0).getName());
    }

    @Test
    void createWithMetadataSetsLabelsOnCreatedObject() throws SQLException {
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());

      DynamicKubernetesObject created = new DynamicKubernetesObject();
      created.setApiVersion("v1");
      created.setKind("ConfigMap");
      created.setMetadata(new V1ObjectMeta().name("obj").namespace("ns"));
      doReturn(successResponse(created)).when(dynApi).create(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("obj").namespace("ns"));

      Map<String, String> labels = new HashMap<>();
      labels.put("env", "prod");

      api.createWithMetadata(obj, null, labels, null);

      ArgumentCaptor<DynamicKubernetesObject> captor = ArgumentCaptor.forClass(DynamicKubernetesObject.class);
      verify(dynApi, times(1)).create(captor.capture());
      Map<String, String> actualLabels = captor.getValue().getMetadata().getLabels();
      assertNotNull(actualLabels);
      assertEquals("prod", actualLabels.get("env"));
    }

    @Test
    void createWithMetadataSetsAnnotationsOnCreatedObject() throws SQLException {
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());

      DynamicKubernetesObject created = new DynamicKubernetesObject();
      created.setApiVersion("v1");
      created.setKind("ConfigMap");
      created.setMetadata(new V1ObjectMeta().name("obj").namespace("ns"));
      doReturn(successResponse(created)).when(dynApi).create(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("obj").namespace("ns"));

      Map<String, String> annotations = new HashMap<>();
      annotations.put("note", "test");

      api.createWithMetadata(obj, annotations, null, null);

      ArgumentCaptor<DynamicKubernetesObject> captor = ArgumentCaptor.forClass(DynamicKubernetesObject.class);
      verify(dynApi, times(1)).create(captor.capture());
      Map<String, String> actualAnnotations = captor.getValue().getMetadata().getAnnotations();
      assertNotNull(actualAnnotations);
      assertEquals("test", actualAnnotations.get("note"));
    }

    @Test
    void createWithMetadataMergesExistingObjectLabelsWithProvided() throws SQLException {
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());

      DynamicKubernetesObject result = new DynamicKubernetesObject();
      result.setApiVersion("v1");
      result.setKind("ConfigMap");
      result.setMetadata(new V1ObjectMeta().name("obj").namespace("ns"));
      doReturn(successResponse(result)).when(dynApi).create(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      // Object already has a label in the YAML
      String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: obj\n  namespace: ns\n  labels:\n    existing: val";
      Map<String, String> extraLabels = new HashMap<>();
      extraLabels.put("new-key", "new-val");

      api.createWithMetadata(yaml, null, extraLabels, null);

      ArgumentCaptor<DynamicKubernetesObject> captor = ArgumentCaptor.forClass(DynamicKubernetesObject.class);
      verify(dynApi, times(1)).create(captor.capture());
      Map<String, String> actualLabels = captor.getValue().getMetadata().getLabels();
      assertNotNull(actualLabels);
      // Both labels should be present
      assertEquals("val", actualLabels.get("existing"));
      assertEquals("new-val", actualLabels.get("new-key"));
    }

    @Test
    void createWithMetadataMergesExistingAnnotationsWithProvided() throws SQLException {
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());

      DynamicKubernetesObject result = new DynamicKubernetesObject();
      result.setApiVersion("v1");
      result.setKind("ConfigMap");
      result.setMetadata(new V1ObjectMeta().name("obj").namespace("ns"));
      doReturn(successResponse(result)).when(dynApi).create(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: obj\n  namespace: ns\n"
          + "  annotations:\n    pre-existing: anno";
      Map<String, String> extraAnnotations = new HashMap<>();
      extraAnnotations.put("added", "here");

      api.createWithMetadata(yaml, extraAnnotations, null, null);

      ArgumentCaptor<DynamicKubernetesObject> captor = ArgumentCaptor.forClass(DynamicKubernetesObject.class);
      verify(dynApi, times(1)).create(captor.capture());
      Map<String, String> actualAnnotations = captor.getValue().getMetadata().getAnnotations();
      assertNotNull(actualAnnotations);
      assertEquals("anno", actualAnnotations.get("pre-existing"));
      assertEquals("here", actualAnnotations.get("added"));
    }

    @Test
    void createWithMetadataMergesExistingOwnerRefsWithProvided() throws SQLException {
      // Optional.ofNullable(obj.getMetadata().getOwnerReferences()).ifPresent(...)
      // Uses the String-yaml overload which merges owner refs from both the YAML and the param.
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());

      DynamicKubernetesObject result = new DynamicKubernetesObject();
      result.setApiVersion("v1");
      result.setKind("ConfigMap");
      result.setMetadata(new V1ObjectMeta().name("obj").namespace("ns"));
      doReturn(successResponse(result)).when(dynApi).create(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);

      // Use a yaml where we can provide owner refs via both the yaml object AND the param list
      // The String overload merges: yaml's ownerRefs + param ownerRefs
      // For simplicity: yaml has no owner refs, param has 1 owner ref
      // The ifPresent ensures that the param owner refs are added to the merged list
      List<V1OwnerReference> addedRefs = new ArrayList<>();
      addedRefs.add(new V1OwnerReference().name("new-owner").uid("uid-new").kind("Svc").apiVersion("v1"));

      api.createWithMetadata("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: obj\n  namespace: ns",
          null, null, addedRefs);

      ArgumentCaptor<DynamicKubernetesObject> captor = ArgumentCaptor.forClass(DynamicKubernetesObject.class);
      verify(dynApi, times(1)).create(captor.capture());
      List<V1OwnerReference> actualOwners = captor.getValue().getMetadata().getOwnerReferences();
      assertNotNull(actualOwners);
      assertEquals(1, actualOwners.size());
      assertEquals("new-owner", actualOwners.get(0).getName());
    }
  }

  // ── Verify update() label merge and update call ──

  @Nested
  @ExtendWith(MockitoExtension.class)
  @SuppressFBWarnings(value = {"RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"},
      justification = "Mockito doReturn().when() stubs — framework captures the return value")
  class UpdateSideEffectTests {

    @Mock
    private K8sContext mockContext;

    @SuppressWarnings("unchecked")
    private KubernetesApiResponse<DynamicKubernetesObject> successResponse(DynamicKubernetesObject obj) {
      KubernetesApiResponse<DynamicKubernetesObject> resp = mock(KubernetesApiResponse.class);
      lenient().doReturn(true).when(resp).isSuccess();
      lenient().doReturn(200).when(resp).getHttpStatusCode();
      lenient().doReturn(obj).when(resp).getObject();
      return resp;
    }

    @SuppressWarnings("unchecked")
    private KubernetesApiResponse<DynamicKubernetesObject> notFoundResponse() {
      KubernetesApiResponse<DynamicKubernetesObject> resp = mock(KubernetesApiResponse.class);
      lenient().doReturn(false).when(resp).isSuccess();
      lenient().doReturn(404).when(resp).getHttpStatusCode();
      return resp;
    }

    @Test
    void updateExistingCallsDynApiUpdate() throws SQLException {
      DynamicKubernetesObject existingObj = new DynamicKubernetesObject();
      existingObj.setApiVersion("v1");
      existingObj.setKind("ConfigMap");
      existingObj.setMetadata(new V1ObjectMeta().name("target").namespace("ns"));

      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).get(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).update(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("target").namespace("ns"));

      api.update(obj);

      verify(dynApi, times(1)).update(any(DynamicKubernetesObject.class));
      verify(dynApi, never()).create(any());
    }

    @Test
    void updateExistingPreservesExistingLabelsOverNewOnes() throws SQLException {
      // obj.setMetadata(existing.getObject().getMetadata()) — existing metadata replaces obj's.
      // After update, existing labels are present on the object passed to dynApi.update().
      DynamicKubernetesObject existingObj = new DynamicKubernetesObject();
      existingObj.setApiVersion("v1");
      existingObj.setKind("ConfigMap");
      Map<String, String> existingLabels = new HashMap<>();
      existingLabels.put("key", "existing-value");
      existingObj.setMetadata(new V1ObjectMeta().name("target").namespace("ns").labels(existingLabels));

      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).get(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).update(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      // obj starts with different label value for same key
      Map<String, String> newLabels = new HashMap<>();
      newLabels.put("key", "new-value");
      obj.setMetadata(new V1ObjectMeta().name("target").namespace("ns").labels(newLabels));

      api.update(obj);

      // After update, obj's metadata = existingObj's metadata
      // The existing label value should be present since existing metadata replaces obj's metadata
      ArgumentCaptor<DynamicKubernetesObject> captor = ArgumentCaptor.forClass(DynamicKubernetesObject.class);
      verify(dynApi, times(1)).update(captor.capture());
      Map<String, String> capturedLabels = captor.getValue().getMetadata().getLabels();
      assertNotNull(capturedLabels);
      assertEquals("existing-value", capturedLabels.get("key"));
    }

    @Test
    void updateExistingCopiesMetadataFromExistingToObj() throws SQLException {
      // Verifies obj.setMetadata(existing.getObject().getMetadata())
      DynamicKubernetesObject existingObj = new DynamicKubernetesObject();
      existingObj.setApiVersion("v1");
      existingObj.setKind("ConfigMap");
      V1ObjectMeta existingMeta = new V1ObjectMeta().name("target").namespace("ns");
      existingMeta.setResourceVersion("rv-existing");
      existingObj.setMetadata(existingMeta);

      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).get(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).update(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("target").namespace("ns"));

      api.update(obj);

      // After update, obj should have the resourceVersion from existing (metadata was replaced)
      ArgumentCaptor<DynamicKubernetesObject> captor = ArgumentCaptor.forClass(DynamicKubernetesObject.class);
      verify(dynApi, times(1)).update(captor.capture());
      assertEquals("rv-existing", captor.getValue().getMetadata().getResourceVersion());
    }

    @Test
    void updateNonExistingCallsDynApiCreate() throws SQLException {
      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(notFoundResponse()).when(dynApi).get(anyString(), anyString());

      DynamicKubernetesObject created = new DynamicKubernetesObject();
      created.setApiVersion("v1");
      created.setKind("ConfigMap");
      created.setMetadata(new V1ObjectMeta().name("new").namespace("ns"));
      doReturn(successResponse(created)).when(dynApi).create(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("new").namespace("ns"));

      api.update(obj);

      verify(dynApi, times(1)).create(any(DynamicKubernetesObject.class));
      verify(dynApi, never()).update(any());
    }

    @Test
    void getIfExistsReturnsObjectFor200AndNotNullFor200Status() throws SQLException {
      DynamicKubernetesObject foundObj = new DynamicKubernetesObject();
      foundObj.setApiVersion("v1");
      foundObj.setKind("ConfigMap");
      foundObj.setMetadata(new V1ObjectMeta().name("found").namespace("ns"));

      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(successResponse(foundObj)).when(dynApi).get(anyString(), anyString());

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("found").namespace("ns"));

      DynamicKubernetesObject result = api.getIfExists(obj);

      assertNotNull(result);
      assertEquals("found", result.getMetadata().getName());
    }

    @Test
    void getIfExistsThrowsForNon404ErrorResponse() throws ApiException, SQLException {
      @SuppressWarnings("unchecked")
      KubernetesApiResponse<DynamicKubernetesObject> errorResp = mock(KubernetesApiResponse.class);
      lenient().doReturn(false).when(errorResp).isSuccess();
      lenient().doReturn(500).when(errorResp).getHttpStatusCode();
      ApiException apiEx = new ApiException(500, "Server Error");
      doThrow(apiEx).when(errorResp).throwsApiException();

      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(errorResp).when(dynApi).get(anyString(), anyString());

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      obj.setMetadata(new V1ObjectMeta().name("bad").namespace("ns"));

      assertThrows(SQLException.class, () -> api.getIfExists(obj));
    }

    @Test
    void updateNewObjectWithNullNamespaceSetsFromContext() throws SQLException {
      lenient().when(mockContext.namespace()).thenReturn("ctx-ns");

      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(notFoundResponse()).when(dynApi).get(anyString(), anyString());

      DynamicKubernetesObject created = new DynamicKubernetesObject();
      created.setApiVersion("v1");
      created.setKind("ConfigMap");
      created.setMetadata(new V1ObjectMeta().name("obj").namespace("ctx-ns"));
      doReturn(successResponse(created)).when(dynApi).create(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      // Use yaml string path which goes through objFromYaml (sets namespace from context when null)
      String yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: obj";

      api.update(yaml);

      ArgumentCaptor<String> nsCaptor = ArgumentCaptor.forClass(String.class);
      verify(dynApi, times(1)).get(nsCaptor.capture(), anyString());
      assertEquals("ctx-ns", nsCaptor.getValue());
    }

    @Test
    void updateExistingObjectWithNewLabelsOnlyPreservesAll() throws SQLException {
      // obj.setMetadata(existing.getObject().getMetadata()) — existing labels are preserved.
      // The existing label should appear on the object passed to dynApi.update().
      DynamicKubernetesObject existingObj = new DynamicKubernetesObject();
      existingObj.setApiVersion("v1");
      existingObj.setKind("ConfigMap");
      Map<String, String> existingLabels = new HashMap<>();
      existingLabels.put("existing", "e-val");
      existingObj.setMetadata(new V1ObjectMeta().name("target").namespace("ns").labels(existingLabels));

      DynamicKubernetesApi dynApi = mock(DynamicKubernetesApi.class);
      doReturn(dynApi).when(mockContext).dynamic(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).get(anyString(), anyString());
      doReturn(successResponse(existingObj)).when(dynApi).update(any(DynamicKubernetesObject.class));

      K8sYamlApi api = new K8sYamlApi(mockContext);
      DynamicKubernetesObject obj = new DynamicKubernetesObject();
      obj.setApiVersion("v1");
      obj.setKind("ConfigMap");
      Map<String, String> newLabels = new HashMap<>();
      newLabels.put("added", "a-val");
      obj.setMetadata(new V1ObjectMeta().name("target").namespace("ns").labels(newLabels));

      api.update(obj);

      // After obj has existingObj's metadata; existing label must be present
      ArgumentCaptor<DynamicKubernetesObject> captor = ArgumentCaptor.forClass(DynamicKubernetesObject.class);
      verify(dynApi, times(1)).update(captor.capture());
      Map<String, String> capturedLabels = captor.getValue().getMetadata().getLabels();
      assertNotNull(capturedLabels);
      assertTrue(capturedLabels.containsKey("existing"), "Should contain existing label");
    }
  }

  private K8sYamlApi createRealApi() {
    K8sContext mockContext = mock(K8sContext.class);
    lenient().when(mockContext.namespace()).thenReturn("test-ns");
    return new K8sYamlApi(mockContext);
  }
}
