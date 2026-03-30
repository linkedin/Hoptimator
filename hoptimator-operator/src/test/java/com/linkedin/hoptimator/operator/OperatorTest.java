package com.linkedin.hoptimator.operator;

import com.google.gson.JsonObject;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
class OperatorTest {

  @Mock
  private ApiClient apiClient;

  private Operator operator;

  // A valid minimal YAML for a K8s object
  private static final String VALID_YAML =
      "apiVersion: foo.org/v1beta1\n" + "kind: FakeJob\n" + "metadata:\n" + "  name: fake-job\n"
          + "  namespace: fake-ns\n";

  // YAML that triggers ScannerException: colon in unquoted value
  private static final String SCANNER_EXCEPTION_YAML = "key: value: with: colons: everywhere:";

  // YAML that triggers ParserException: invalid document structure
  private static final String PARSER_EXCEPTION_YAML = "--- invalid\n--- also invalid\n---";

  // Valid YAML with no namespace — triggers null namespace in isReady/isFailed
  private static final String NO_NAMESPACE_YAML =
      "apiVersion: foo.org/v1beta1\n" + "kind: FakeJob\n" + "metadata:\n" + "  name: fake-job\n";

  // Valid YAML with no metadata at all — triggers null metadata
  private static final String NO_METADATA_YAML = "apiVersion: v1\nkind: ConfigMap";

  @BeforeEach
  void setUp() {
    Properties props = new Properties();
    props.setProperty("test.key", "test.value");
    operator = new Operator("fake-namespace", apiClient, props);
    operator.registerApi("FakeJob", "fakejob", "fakejobs", "foo.org", "v1beta1");
  }

  @Test
  void testIsReadyReturnsFalseOnScannerException() {
    // isReady(yaml) must return false rather than throw when YAML triggers a ScannerException.
    boolean result = operator.isReady(SCANNER_EXCEPTION_YAML);
    assertFalse(result);
  }

  @Test
  void testIsReadyReturnsFalseOnParserException() {
    // isReady(yaml) must return false rather than throw when YAML triggers a ParserException.
    boolean result = operator.isReady(PARSER_EXCEPTION_YAML);
    assertFalse(result);
  }

  @Test
  void testIsFailedReturnsFalseOnScannerException() {
    // Same protection needed for isFailed(): should not throw on malformed YAML.
    boolean result = operator.isFailed(SCANNER_EXCEPTION_YAML);
    assertFalse(result);
  }

  @Test
  void testIsFailedReturnsFalseOnParserException() {
    // Same protection needed for isFailed(): should not throw on malformed YAML.
    boolean result = operator.isFailed(PARSER_EXCEPTION_YAML);
    assertFalse(result);
  }

  @Test
  void testIsReadyReturnsFalseOnNullNamespace() {
    // isReady(yaml) must return false rather than throw when the YAML has no namespace.
    boolean result = operator.isReady(NO_NAMESPACE_YAML);
    assertFalse(result);
  }

  @Test
  void testIsFailedReturnsFalseOnNullNamespace() {
    boolean result = operator.isFailed(NO_NAMESPACE_YAML);
    assertFalse(result);
  }

  @Test
  void testIsReadyReturnsFalseOnNullMetadata() {
    boolean result = operator.isReady(NO_METADATA_YAML);
    assertFalse(result);
  }

  @Test
  void testIsFailedReturnsFalseOnNullMetadata() {
    boolean result = operator.isFailed(NO_METADATA_YAML);
    assertFalse(result);
  }

  @Test
  void propertiesReturnsConfiguredProperties() {
    assertEquals("test.value", operator.properties().getProperty("test.key"));
  }

  @Test
  void failureRetryDurationReturnsFiveMinutes() {
    assertEquals(Duration.ofMinutes(5), operator.failureRetryDuration());
  }

  @Test
  void pendingRetryDurationReturnsOneMinute() {
    assertEquals(Duration.ofMinutes(1), operator.pendingRetryDuration());
  }

  @Test
  void resyncPeriodReturnsTenMinutes() {
    assertEquals(Duration.ofMinutes(10), operator.resyncPeriod());
  }

  @Test
  void apiInfoThrowsForUnregisteredApi() {
    assertThrows(IllegalArgumentException.class, () -> operator.apiInfo("unknown/v1/Foo"));
  }

  @Test
  void registerApiWithoutWatchMakesApiInfoAvailable() {
    operator.registerApi("Foo", "foo", "foos", "example.com", "v1");
    Operator.ApiInfo<?, ?> info = operator.apiInfo("example.com/v1/Foo");
    assertNotNull(info);
    assertEquals("Foo", info.kind());
    assertEquals("foo", info.singular());
    assertEquals("foos", info.plural());
    assertEquals("example.com", info.group());
    assertEquals("v1", info.version());
  }

  @Test
  void apiInfoGroupVersionKind() {
    Operator.ApiInfo<KubernetesObject, KubernetesListObject> info =
        new Operator.ApiInfo<>("Foo", "foo", "foos", "example.com", "v1",
            KubernetesObject.class, KubernetesListObject.class);
    assertEquals("example.com/v1/Foo", info.groupVersionKind());
    assertEquals("example.com/v1", info.apiVersion());
    assertEquals(KubernetesObject.class, info.type());
    assertEquals(KubernetesListObject.class, info.list());
  }

  @Test
  void isReadyReturnsTrueWhenStatusReadyIsTrue() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("ready", true);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isReady(obj));
  }

  @Test
  void isReadyReturnsFalseWhenStatusReadyIsFalse() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("ready", false);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertFalse(Operator.isReady(obj));
  }

  @Test
  void isReadyReturnsTrueWhenStateIsRunning() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("state", "RUNNING");
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isReady(obj));
  }

  @Test
  void isReadyReturnsFalseWhenStateIsPending() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("state", "PENDING");
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertFalse(Operator.isReady(obj));
  }

  @Test
  void isReadyReturnsTrueWhenJobStatusStateIsFinished() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    JsonObject jobStatus = new JsonObject();
    jobStatus.addProperty("state", "FINISHED");
    status.add("jobStatus", jobStatus);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isReady(obj));
  }

  @Test
  void isReadyReturnsTrueWhenNoStatusField() {
    JsonObject raw = new JsonObject();
    raw.addProperty("kind", "Foo");
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isReady(obj));
  }

  @Test
  void isReadyReturnsFalseWhenObjIsNull() {
    assertFalse(Operator.isReady((DynamicKubernetesObject) null));
  }

  @Test
  void isFailedReturnsTrueWhenStatusFailedIsTrue() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("failed", true);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isFailed(obj));
  }

  @Test
  void isFailedReturnsFalseWhenStatusFailedIsFalse() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("failed", false);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertFalse(Operator.isFailed(obj));
  }

  @Test
  void isFailedReturnsTrueWhenStateIsFailed() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("state", "FAILED");
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isFailed(obj));
  }

  @Test
  void isFailedReturnsTrueWhenStateIsError() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("state", "ERROR");
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isFailed(obj));
  }

  @Test
  void isFailedReturnsTrueWhenJobStatusStateIsFailed() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    JsonObject jobStatus = new JsonObject();
    jobStatus.addProperty("state", "FAILED");
    status.add("jobStatus", jobStatus);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isFailed(obj));
  }

  @Test
  void isFailedReturnsFalseWhenNoStatusField() {
    JsonObject raw = new JsonObject();
    raw.addProperty("kind", "Foo");
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertFalse(Operator.isFailed(obj));
  }

  @Test
  void isFailedReturnsFalseWhenObjIsNull() {
    assertFalse(Operator.isFailed((DynamicKubernetesObject) null));
  }

  @Test
  void constructorWithoutProperties() {
    Operator op = new Operator("ns", apiClient);
    assertNotNull(op.properties());
    assertTrue(op.properties().isEmpty());
  }

  @Test
  void informerFactoryNotNull() {
    assertNotNull(operator.informerFactory());
  }

  @Test
  void apiInfoGenericCreatesApi() {
    Operator.ApiInfo<KubernetesObject, KubernetesListObject> info =
        new Operator.ApiInfo<>("Foo", "foo", "foos", "example.com", "v1",
            KubernetesObject.class, KubernetesListObject.class);
    assertNotNull(info.generic(apiClient));
  }

  @Test
  void apiInfoSingularReturnsSingular() {
    Operator.ApiInfo<KubernetesObject, KubernetesListObject> info =
        new Operator.ApiInfo<>("Foo", "foo", "foos", "example.com", "v1",
            KubernetesObject.class, KubernetesListObject.class);
    assertEquals("foo", info.singular());
  }

  @Test
  void isReadyReturnsTrueWhenStateIsReady() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("state", "READY");
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isReady(obj));
  }

  @Test
  void isReadyReturnsTrueWhenStateIsFinished() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("state", "FINISHED");
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isReady(obj));
  }

  @Test
  void isReadyReturnsFalseWhenRawIsNull() {
    DynamicKubernetesObject obj = new DynamicKubernetesObject(null);
    assertFalse(Operator.isReady(obj));
  }

  @Test
  void isFailedReturnsTrueWhenJobStatusStateIsError() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    JsonObject jobStatus = new JsonObject();
    jobStatus.addProperty("state", "ERROR");
    status.add("jobStatus", jobStatus);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isFailed(obj));
  }

  @Test
  void isFailedReturnsFalseWhenRawIsNull() {
    DynamicKubernetesObject obj = new DynamicKubernetesObject(null);
    assertFalse(Operator.isFailed(obj));
  }

  @Test
  void isReadyReturnsFalseWhenJobStatusStateIsPending() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    JsonObject jobStatus = new JsonObject();
    jobStatus.addProperty("state", "PENDING");
    status.add("jobStatus", jobStatus);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertFalse(Operator.isReady(obj));
  }

  @Test
  void isFailedReturnsFalseWhenJobStatusStateIsRunning() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    JsonObject jobStatus = new JsonObject();
    jobStatus.addProperty("state", "RUNNING");
    status.add("jobStatus", jobStatus);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertFalse(Operator.isFailed(obj));
  }

  @Test
  void registerApiWithoutWatchMultipleApis() {
    operator.registerApi("Bar", "bar", "bars", "test.io", "v2");
    operator.registerApi("Baz", "baz", "bazzes", "test.io", "v1");
    Operator.ApiInfo<?, ?> bar = operator.apiInfo("test.io/v2/Bar");
    Operator.ApiInfo<?, ?> baz = operator.apiInfo("test.io/v1/Baz");
    assertEquals("Bar", bar.kind());
    assertEquals("Baz", baz.kind());
  }

  @Test
  void apiInfoPlural() {
    Operator.ApiInfo<KubernetesObject, KubernetesListObject> info =
        new Operator.ApiInfo<>("Foo", "foo", "foos", "example.com", "v1",
            KubernetesObject.class, KubernetesListObject.class);
    assertEquals("foos", info.plural());
    assertEquals("example.com", info.group());
    assertEquals("v1", info.version());
  }

  @Test
  void apiForStringDelegatesToApiInfo() {
    operator.registerApi("Bar", "bar", "bars", "test.io", "v1");
    assertNotNull(operator.apiFor("test.io/v1/Bar"));
  }

  @Test
  void registerApiOverwritesExisting() {
    operator.registerApi("Foo", "foo", "foos", "g", "v1");
    operator.registerApi("Foo", "foo", "foos2", "g", "v1");
    Operator.ApiInfo<?, ?> info = operator.apiInfo("g/v1/Foo");
    assertEquals("foos2", info.plural());
  }

  @Test
  void isReadyStaticReturnsTrueWhenJobStatusStateIsReady() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    JsonObject jobStatus = new JsonObject();
    jobStatus.addProperty("state", "READY");
    status.add("jobStatus", jobStatus);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isReady(obj));
  }

  @Test
  void isReadyStaticReturnsTrueWhenJobStatusStateIsRunning() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    JsonObject jobStatus = new JsonObject();
    jobStatus.addProperty("state", "RUNNING");
    status.add("jobStatus", jobStatus);
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertTrue(Operator.isReady(obj));
  }

  @Test
  void isFailedReturnsFalseWhenStateIsNotFailed() {
    JsonObject raw = new JsonObject();
    JsonObject status = new JsonObject();
    status.addProperty("state", "RUNNING");
    raw.add("status", status);
    DynamicKubernetesObject obj = new DynamicKubernetesObject(raw);
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("ns"));

    assertFalse(Operator.isFailed(obj));
  }
}
