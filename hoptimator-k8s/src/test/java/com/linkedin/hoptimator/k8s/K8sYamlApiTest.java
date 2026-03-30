package com.linkedin.hoptimator.k8s;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;


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

  private K8sYamlApi createRealApi() {
    K8sContext mockContext = mock(K8sContext.class);
    lenient().when(mockContext.namespace()).thenReturn("test-ns");
    return new K8sYamlApi(mockContext);
  }
}
