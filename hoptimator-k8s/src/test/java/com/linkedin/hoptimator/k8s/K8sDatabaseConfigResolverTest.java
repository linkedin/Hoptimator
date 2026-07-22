package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.options.ListOptions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class K8sDatabaseConfigResolverTest {

  private static final String NAMESPACE = "test-ns";

  /** A K8sContext whose {@code Database} list returns {@code dbs} (Databases are namespaced). */
  private static K8sContext contextReturning(V1alpha1Database... dbs) {
    V1alpha1DatabaseList list = new V1alpha1DatabaseList();
    list.setItems(Arrays.asList(dbs));
    KubernetesApiResponse<V1alpha1DatabaseList> resp = mockResponse();
    when(resp.getObject()).thenReturn(list);
    return contextListing(resp);
  }

  /** A K8sContext whose {@code Database} list fails with an API error (non-transient status). */
  private static K8sContext contextWithListFailure() {
    KubernetesApiResponse<V1alpha1DatabaseList> resp = mockResponse();
    when(resp.getHttpStatusCode()).thenReturn(500);
    try {
      doThrow(new ApiException("boom")).when(resp).throwsApiException();
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
    return contextListing(resp);
  }

  @SuppressWarnings("unchecked")
  private static KubernetesApiResponse<V1alpha1DatabaseList> mockResponse() {
    return mock(KubernetesApiResponse.class);
  }

  private static K8sContext contextListing(KubernetesApiResponse<V1alpha1DatabaseList> resp) {
    K8sContext context = mock(K8sContext.class);
    @SuppressWarnings("unchecked")
    GenericKubernetesApi<V1alpha1Database, V1alpha1DatabaseList> generic = mock(GenericKubernetesApi.class);
    when(context.namespace()).thenReturn(NAMESPACE);
    when(context.generic(K8sApiEndpoints.DATABASES)).thenReturn(generic);
    when(generic.list(eq(NAMESPACE), any(ListOptions.class))).thenReturn(resp);
    return context;
  }

  private static K8sDatabaseConfigResolver resolver(V1alpha1Database... dbs) {
    return new K8sDatabaseConfigResolver(new Properties(), contextReturning(dbs));
  }

  private static V1alpha1Database db(String name, String url, String catalog, String schema) {
    return new V1alpha1Database()
        .metadata(new V1ObjectMeta().name(name))
        .spec(new V1alpha1DatabaseSpec().url(url).catalog(catalog).schema(schema));
  }

  @Test
  void databasePropertiesReturnsNullWhenNoCatalogOrSchema() throws Exception {
    assertThat(resolver().databaseProperties(null, null, "jdbc:kafka://")).isNull();
  }

  @Test
  void databasePropertiesParsesUrlForSchemaStyleDatabase() throws Exception {
    V1alpha1Database kafka = db("kafka-database",
        "jdbc:kafka://bootstrap.servers=localhost:9092", null, "KAFKA");

    Properties props = resolver(kafka).databaseProperties(null, "KAFKA", "jdbc:kafka://");

    assertThat(props).isNotNull();
    assertThat(props.getProperty("bootstrap.servers")).isEqualTo("localhost:9092");
    // joinedUrl injects the CRD name as database=<name>.
    assertThat(props.getProperty("database")).isEqualTo("kafka-database");
  }

  @Test
  void databasePropertiesReturnsNullWhenUrlDoesNotMatchPrefix() throws Exception {
    V1alpha1Database venice = db("venice", "jdbc:venice://clusters=venice-cluster0", null, "VENICE");

    // Database exists for VENICE but the requested prefix is a different store type.
    assertThat(resolver(venice).databaseProperties(null, "VENICE", "jdbc:kafka://")).isNull();
  }

  @Test
  void databasePropertiesReturnsNullWhenNoDatabaseMatches() throws Exception {
    assertThat(resolver().databaseProperties(null, "MISSING", "jdbc:kafka://")).isNull();
  }

  @Test
  void databaseNameReturnsCrdNameForSchemaStyle() throws Exception {
    V1alpha1Database kafka = db("kafka-database",
        "jdbc:kafka://bootstrap.servers=localhost:9092", null, "KAFKA");

    assertThat(resolver(kafka).databaseName(Arrays.asList("KAFKA", "my_topic"))).isEqualTo("kafka-database");
  }

  @Test
  void databaseNameMatchesCatalogStyleByCatalog() throws Exception {
    V1alpha1Database mysql = db("mysql", "jdbc:mysql-hoptimator://url=jdbc:mysql://localhost:3306",
        "MYSQL", null);

    // Catalog-style: three-segment path [CATALOG, SCHEMA, TABLE] matches on the catalog CRD.
    assertThat(resolver(mysql).databaseName(Arrays.asList("MYSQL", "test_database", "orders")))
        .isEqualTo("mysql");
  }

  @Test
  void databaseNameThrowsWhenNoDatabaseRegistered() {
    assertThatThrownBy(() -> resolver().databaseName(Arrays.asList("UNKNOWN", "t")))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("No Database is registered");
  }

  @Test
  void databaseNameThrowsWhenListFails() {
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(new Properties(), contextWithListFailure());

    // A failed list must surface as an error, not be swallowed into "no Database registered".
    assertThatThrownBy(() -> resolver.databaseName(Arrays.asList("KAFKA", "t")))
        .isInstanceOf(SQLException.class)
        .hasMessageNotContaining("No Database is registered");
  }

  @Test
  void databasePropertiesThrowsWhenListFails() {
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(new Properties(), contextWithListFailure());

    // Must fail loudly rather than returning null (which reads as "no config, deploy nothing").
    // A 500 from the API surfaces as a (non-transient) SQLException.
    assertThatThrownBy(() -> resolver.databaseProperties(null, "KAFKA", "jdbc:kafka://"))
        .isInstanceOf(SQLException.class);
  }

  @Test
  void listConnectionFailureIsNormalizedToTransient() {
    // K8sApi.list() surfaces a connectivity failure as an unchecked exception; it must be normalized
    // to a typed SQLTransientException so callers can classify it as a retryable transient failure.
    K8sContext context = mock(K8sContext.class);
    @SuppressWarnings("unchecked")
    GenericKubernetesApi<V1alpha1Database, V1alpha1DatabaseList> generic = mock(GenericKubernetesApi.class);
    when(context.namespace()).thenReturn(NAMESPACE);
    when(context.generic(K8sApiEndpoints.DATABASES)).thenReturn(generic);
    when(generic.list(eq(NAMESPACE), any(ListOptions.class)))
        .thenThrow(new IllegalStateException("java.net.UnknownHostException: k8s.invalid"));
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(new Properties(), context);

    assertThatThrownBy(() -> resolver.databaseProperties(null, "KAFKA", "jdbc:kafka://"))
        .isInstanceOf(SQLTransientException.class)
        .hasMessageContaining("Could not reach Kubernetes");
  }

  @Test
  void listsDatabasesOncePerResolverAcrossMultipleResolutions() throws Exception {
    V1alpha1DatabaseList list = new V1alpha1DatabaseList();
    list.setItems(Collections.singletonList(
        db("kafka-database", "jdbc:kafka://bootstrap.servers=localhost:9092", null, "KAFKA")));
    KubernetesApiResponse<V1alpha1DatabaseList> resp = mockResponse();
    when(resp.getObject()).thenReturn(list);
    @SuppressWarnings("unchecked")
    GenericKubernetesApi<V1alpha1Database, V1alpha1DatabaseList> generic = mock(GenericKubernetesApi.class);
    K8sContext context = mock(K8sContext.class);
    when(context.namespace()).thenReturn(NAMESPACE);
    when(context.generic(K8sApiEndpoints.DATABASES)).thenReturn(generic);
    when(generic.list(eq(NAMESPACE), any(ListOptions.class))).thenReturn(resp);
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(new Properties(), context);

    // Several resolutions on the same resolver, as a logical table's tiers would trigger.
    resolver.databaseName(Arrays.asList("KAFKA", "t1"));
    resolver.databaseProperties(null, "KAFKA", "jdbc:kafka://");
    resolver.databaseName(Arrays.asList("KAFKA", "t2"));

    // The per-resolver cache means the Database CRDs are listed only once.
    verify(generic, times(1)).list(eq(NAMESPACE), any(ListOptions.class));
  }
}
