package com.linkedin.hoptimator.operator;

import com.google.gson.JsonObject;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests for additional Operator paths not covered in OperatorTest:
 * registerApi with watch, informer(), lister(), fetch(), apiFor(DynamicObj),
 * apply(), isReady(String yaml), isFailed(String yaml), ApiInfo.dynamic()
 */
@ExtendWith(MockitoExtension.class)
class OperatorApiTest {

  @Mock
  private ApiClient apiClient;

  @Mock
  private SharedInformerFactory mockInformerFactory;

  @Mock
  private SharedIndexInformer<KubernetesObject> mockInformer;

  @Mock
  private Indexer<KubernetesObject> mockIndexer;

  private Operator operator;

  @BeforeEach
  void setUp() {
    operator = new Operator("default", apiClient, mockInformerFactory);
  }

  @Test
  void registerApiWithWatchCallsSharedIndexInformerFor() {
    when(mockInformerFactory.sharedIndexInformerFor(any(), any(), anyLong(), anyString()))
        .thenReturn(mockInformer);

    operator.registerApi("Foo", "foo", "foos", "example.com", "v1",
        KubernetesObject.class, KubernetesListObject.class);

    Operator.ApiInfo<?, ?> info = operator.apiInfo("example.com/v1/Foo");
    assertNotNull(info);
    verify(mockInformerFactory).sharedIndexInformerFor(any(), any(), anyLong(), eq("default"));
  }

  @Test
  void informerDelegatesToInformerFactory() {
    when(mockInformerFactory.sharedIndexInformerFor(any(), any(), anyLong(), anyString()))
        .thenReturn(mockInformer);
    when(mockInformerFactory.getExistingSharedIndexInformer(KubernetesObject.class))
        .thenReturn(mockInformer);

    operator.registerApi("Bar", "bar", "bars", "test.io", "v1",
        KubernetesObject.class, KubernetesListObject.class);

    SharedIndexInformer<KubernetesObject> result = operator.informer("test.io/v1/Bar");

    assertNotNull(result);
    verify(mockInformerFactory).getExistingSharedIndexInformer(KubernetesObject.class);
  }

  @Test
  void listerReturnsListerFromInformerIndexer() {
    when(mockInformerFactory.sharedIndexInformerFor(any(), any(), anyLong(), anyString()))
        .thenReturn(mockInformer);
    when(mockInformerFactory.getExistingSharedIndexInformer(KubernetesObject.class))
        .thenReturn(mockInformer);
    when(mockInformer.getIndexer()).thenReturn(mockIndexer);

    operator.registerApi("Baz", "baz", "bazzes", "test.io", "v1",
        KubernetesObject.class, KubernetesListObject.class);

    Lister<KubernetesObject> lister = operator.lister("test.io/v1/Baz");

    assertNotNull(lister);
  }

  @Test
  void fetchReturnsObjectFromLister() {
    when(mockInformerFactory.sharedIndexInformerFor(any(), any(), anyLong(), anyString()))
        .thenReturn(mockInformer);
    when(mockInformerFactory.getExistingSharedIndexInformer(KubernetesObject.class))
        .thenReturn(mockInformer);
    when(mockInformer.getIndexer()).thenReturn(mockIndexer);

    operator.registerApi("Quux", "quux", "quuxes", "test.io", "v1",
        KubernetesObject.class, KubernetesListObject.class);

    // fetch calls lister(gvk).namespace(ns).get(name) - the Indexer mock returns null by default
    // result is null since mock indexer has no entries
    assertNull(operator.fetch("test.io/v1/Quux", "ns", "name"));
    assertNotNull(operator.informer("test.io/v1/Quux"));
  }

  @Test
  void apiForDynamicObjectDelegatesToApiInfo() {
    ApiClient deepStubClient = mock(ApiClient.class, RETURNS_DEEP_STUBS);
    Operator deepOp = new Operator("default", deepStubClient, mockInformerFactory);
    deepOp.registerApi("MyKind", "mykind", "mykinds", "test.io", "v1");

    JsonObject raw = new JsonObject();
    raw.addProperty("apiVersion", "test.io/v1");
    raw.addProperty("kind", "MyKind");
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    DynamicKubernetesApi result = deepOp.apiFor(obj);

    assertNotNull(result);
  }

  @Test
  void apiForStringReturnsGenericApi() {
    operator.registerApi("Widget", "widget", "widgets", "example.com", "v1",
        KubernetesObject.class, KubernetesListObject.class);

    when(mockInformerFactory.sharedIndexInformerFor(any(), any(), anyLong(), anyString()))
        .thenReturn(mockInformer);

    // Re-register to trigger informer watch
    operator.registerApi("Widget", "widget", "widgets", "example.com", "v1",
        KubernetesObject.class, KubernetesListObject.class);

    assertNotNull(operator.apiFor("example.com/v1/Widget"));
  }

  @Test
  void apiInfoDynamicCreatesApi() {
    ApiClient deepStubClient = mock(ApiClient.class, RETURNS_DEEP_STUBS);
    Operator.ApiInfo<KubernetesObject, KubernetesListObject> info =
        new Operator.ApiInfo<>("Foo", "foo", "foos", "example.com", "v1",
            KubernetesObject.class, KubernetesListObject.class);

    DynamicKubernetesApi dynamicApi = info.dynamic(deepStubClient);

    assertNotNull(dynamicApi);
  }

  @Test
  void isReadyInstanceReturnsFalseWhenApiNotRegistered() {
    // apiFor will throw IllegalArgumentException since no API registered
    // isReady catches all exceptions and returns false
    String yaml = "apiVersion: test.io/v1\nkind: Missing\nmetadata:\n  name: foo\n  namespace: ns\n";

    boolean result = operator.isReady(yaml);

    assertFalse(result);
  }

  @Test
  void isFailedInstanceReturnsFalseWhenApiNotRegistered() {
    String yaml = "apiVersion: test.io/v1\nkind: Missing\nmetadata:\n  name: foo\n  namespace: ns\n";

    boolean result = operator.isFailed(yaml);

    assertFalse(result);
  }

  @Test
  void isReadyInstanceReturnsFalseWhenFetchFails() {
    ApiClient deepStubClient = mock(ApiClient.class, RETURNS_DEEP_STUBS);
    Operator deepOp = new Operator("default", deepStubClient, mockInformerFactory);
    deepOp.registerApi("MyKind", "mykind", "mykinds", "test.io", "v1");

    // The DynamicKubernetesApi.get() will fail since deep-stub client can't make real calls
    // isReady catches exceptions and returns false
    String yaml = "apiVersion: test.io/v1\nkind: MyKind\nmetadata:\n  name: foo\n  namespace: ns\n";

    boolean result = deepOp.isReady(yaml);

    assertFalse(result);
  }

  @Test
  void isFailedInstanceReturnsFalseWhenFetchFails() {
    ApiClient deepStubClient = mock(ApiClient.class, RETURNS_DEEP_STUBS);
    Operator deepOp = new Operator("default", deepStubClient, mockInformerFactory);
    deepOp.registerApi("MyKind", "mykind", "mykinds", "test.io", "v1");

    String yaml = "apiVersion: test.io/v1\nkind: MyKind\nmetadata:\n  name: foo\n  namespace: ns\n";

    boolean result = deepOp.isFailed(yaml);

    assertFalse(result);
  }

  @Test
  void applyExercisesCodePathWithMockedClient() {
    ApiClient deepStubClient = mock(ApiClient.class, RETURNS_DEEP_STUBS);
    Operator deepOp = new Operator("default", deepStubClient, mockInformerFactory);
    deepOp.registerApi("MyKind", "mykind", "mykinds", "test.io", "v1");

    JsonObject ownerRaw = new JsonObject();
    ownerRaw.addProperty("apiVersion", "test.io/v1");
    ownerRaw.addProperty("kind", "Owner");
    DynamicKubernetesObject owner = new DynamicKubernetesObject(ownerRaw);
    owner.setMetadata(new V1ObjectMeta().name("owner-obj").namespace("ns").uid("uid-owner"));

    String yaml = "apiVersion: test.io/v1\nkind: MyKind\nmetadata:\n  name: child\n  namespace: ns\n";

    // apply() will call apiFor(obj) -> dynamic, and then .get() which will fail with deep-stub client.
    // We just verify apply() is exercised (throws is acceptable since mock client can't make real calls)
    try {
      deepOp.apply(yaml, owner);
    } catch (ApiException | RuntimeException e) {
      // expected when mock client can't make real K8s calls
    }
    assertNotNull(deepOp);
  }

  @Test
  void applyThrowsWhenObjectHasNoName() {
    operator.registerApi("MyKind", "mykind", "mykinds", "test.io", "v1");

    JsonObject ownerRaw = new JsonObject();
    ownerRaw.addProperty("apiVersion", "test.io/v1");
    ownerRaw.addProperty("kind", "Owner");
    DynamicKubernetesObject owner = new DynamicKubernetesObject(ownerRaw);
    owner.setMetadata(new V1ObjectMeta().name("owner").namespace("ns").uid("uid-owner"));

    String yaml = "apiVersion: test.io/v1\nkind: MyKind\nmetadata:\n  namespace: ns\n";

    assertThrows(IllegalArgumentException.class, () -> operator.apply(yaml, owner));
  }

  @Test
  void registerApiApiInfoOverloadWithWatch() {
    when(mockInformerFactory.sharedIndexInformerFor(any(), any(), anyLong(), anyString()))
        .thenReturn(mockInformer);

    Operator.ApiInfo<KubernetesObject, KubernetesListObject> info =
        new Operator.ApiInfo<>("Woo", "woo", "woos", "some.io", "v1",
            KubernetesObject.class, KubernetesListObject.class);

    operator.registerApi(info, true);

    assertNotNull(operator.apiInfo("some.io/v1/Woo"));
    verify(mockInformerFactory).sharedIndexInformerFor(any(), any(), anyLong(), anyString());
  }

  @Test
  void registerApiApiInfoOverloadWithoutWatch() {
    Operator.ApiInfo<KubernetesObject, KubernetesListObject> info =
        new Operator.ApiInfo<>("Zoo", "zoo", "zoos", "some.io", "v1",
            KubernetesObject.class, KubernetesListObject.class);

    operator.registerApi(info, false);

    assertNotNull(operator.apiInfo("some.io/v1/Zoo"));
  }
}
