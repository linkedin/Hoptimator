package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateSpec.MethodsEnum;
import io.kubernetes.client.common.KubernetesType;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLNonTransientException;
import java.sql.SQLTransientException;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class K8sUtilsTest {

  @Test
  void canonicalizeNameSingleString() {
    assertEquals("hello", K8sUtils.canonicalizeName("Hello"));
  }

  @Test
  void canonicalizeNameReplacesUnderscoresAndDollarSigns() {
    assertEquals("my-table", K8sUtils.canonicalizeName("my_$table"));
  }

  @Test
  void canonicalizeNameWithDatabaseAndTable() {
    assertEquals("mydb-mytable", K8sUtils.canonicalizeName("myDB", "myTable"));
  }

  @Test
  void canonicalizeNameWithNullDatabase() {
    assertEquals("mytable", K8sUtils.canonicalizeName(null, "myTable"));
  }

  @Test
  void canonicalizeNameFromCollection() {
    assertEquals("a-b-c", K8sUtils.canonicalizeName(Arrays.asList("A", "B", "C")));
  }

  @Test
  void canonicalizeNameFromCollectionWithNulls() {
    assertEquals("a-c", K8sUtils.canonicalizeName(Arrays.asList("A", null, "C")));
  }

  @Test
  void checkK8sNameValidName() {
    K8sUtils.checkK8sName("my-valid-name");
  }

  @Test
  void checkK8sNameNullThrows() {
    assertThrows(IllegalArgumentException.class, () -> K8sUtils.checkK8sName(null));
  }

  @Test
  void checkK8sNameEmptyThrows() {
    assertThrows(IllegalArgumentException.class, () -> K8sUtils.checkK8sName(""));
  }

  @Test
  void checkK8sNameTooLongThrows() {
    String longName = "a".repeat(64);
    assertThrows(IllegalArgumentException.class, () -> K8sUtils.checkK8sName(longName));
  }

  @Test
  void checkK8sNameMaxLengthOk() {
    String maxName = "a".repeat(63);
    K8sUtils.checkK8sName(maxName);
  }

  @Test
  void checkK8sNameIllegalCharactersThrows() {
    assertThrows(IllegalArgumentException.class, () -> K8sUtils.checkK8sName("my_name"));
  }

  @Test
  void checkK8sNameStartsWithNumberThrows() {
    assertThrows(IllegalArgumentException.class, () -> K8sUtils.checkK8sName("1abc"));
  }

  @Test
  void checkK8sNameEndsWithDashThrows() {
    assertThrows(IllegalArgumentException.class, () -> K8sUtils.checkK8sName("abc-"));
  }

  @Test
  void guessPluralsFromKind() {
    assertEquals("pipelines", K8sUtils.guessPlural("Pipeline"));
  }

  @Test
  void guessPluralEndsWithY() {
    assertEquals("policies", K8sUtils.guessPlural("Policy"));
  }

  @Test
  void guessPluralFromKubernetesType() {
    KubernetesType type = mock(KubernetesType.class);
    when(type.getKind()).thenReturn("Deployment");
    assertEquals("deployments", K8sUtils.guessPlural(type));
  }

  @Test
  void methodReturnsScanForSource() {
    Source source = mock(Source.class);
    assertEquals(MethodsEnum.SCAN, K8sUtils.method(source));
  }

  @Test
  void methodReturnsModifyForSink() {
    Sink sink = mock(Sink.class);
    assertEquals(MethodsEnum.MODIFY, K8sUtils.method(sink));
  }

  @Test
  void checkResponseSuccessDoesNotThrow() throws Exception {
    KubernetesApiResponse<?> resp = mock(KubernetesApiResponse.class);
    K8sUtils.checkResponse("test", resp);
  }

  static Stream<Arguments> transientStatusCodes() {
    return Stream.of(
        Arguments.of(404),
        Arguments.of(408),
        Arguments.of(409),
        Arguments.of(410),
        Arguments.of(412)
    );
  }

  @ParameterizedTest
  @MethodSource("transientStatusCodes")
  void checkResponseTransientCodesThrowSQLTransientException(int statusCode) throws Exception {
    KubernetesApiResponse<?> resp = mock(KubernetesApiResponse.class);
    doThrow(new ApiException("error")).when(resp).throwsApiException();
    when(resp.getHttpStatusCode()).thenReturn(statusCode);
    assertThrows(SQLTransientException.class, () -> K8sUtils.checkResponse("test", resp));
  }

  @Test
  void checkResponseNonTransientCodeThrowsSQLNonTransientException() throws Exception {
    KubernetesApiResponse<?> resp = mock(KubernetesApiResponse.class);
    doThrow(new ApiException("error")).when(resp).throwsApiException();
    when(resp.getHttpStatusCode()).thenReturn(500);
    assertThrows(SQLNonTransientException.class, () -> K8sUtils.checkResponse("test", resp));
  }

  @Test
  void overrideNamespaceFromContextSetsNamespaceWhenNull() {
    K8sContext context = mock(K8sContext.class);
    when(context.namespace()).thenReturn("my-namespace");
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setMetadata(new V1ObjectMeta().name("test"));

    DynamicKubernetesObject result = K8sUtils.overrideNamespaceFromContext(context, obj);

    assertEquals("my-namespace", result.getMetadata().getNamespace());
  }

  @Test
  void overrideNamespaceFromContextKeepsExistingNamespace() {
    K8sContext context = mock(K8sContext.class);
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("existing-ns"));

    DynamicKubernetesObject result = K8sUtils.overrideNamespaceFromContext(context, obj);

    assertEquals("existing-ns", result.getMetadata().getNamespace());
  }
}
