package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;


class K8sDatabaseConfigResolverTest {

  @SuppressWarnings("unchecked")
  private static K8sApi<V1alpha1Database, V1alpha1DatabaseList> apiReturning(V1alpha1Database... dbs)
      throws SQLException {
    K8sApi<V1alpha1Database, V1alpha1DatabaseList> api = mock(K8sApi.class);
    doReturn(Arrays.asList(dbs)).when(api).list();
    return api;
  }

  private static V1alpha1Database db(String name, String url, String catalog, String schema) {
    return new V1alpha1Database()
        .metadata(new V1ObjectMeta().name(name))
        .spec(new V1alpha1DatabaseSpec().url(url).catalog(catalog).schema(schema));
  }

  @Test
  void databasePropertiesReturnsNullWhenNoCatalogOrSchema() throws SQLException {
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(apiReturning());

    assertThat(resolver.databaseProperties(null, null, "jdbc:kafka://")).isNull();
  }

  @Test
  void databasePropertiesParsesUrlForSchemaStyleDatabase() throws SQLException {
    V1alpha1Database kafka = db("kafka-database",
        "jdbc:kafka://bootstrap.servers=localhost:9092", null, "KAFKA");
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(apiReturning(kafka));

    Properties props = resolver.databaseProperties(null, "KAFKA", "jdbc:kafka://");

    assertThat(props).isNotNull();
    assertThat(props.getProperty("bootstrap.servers")).isEqualTo("localhost:9092");
    // joinedUrl injects the CRD name as database=<name>.
    assertThat(props.getProperty("database")).isEqualTo("kafka-database");
  }

  @Test
  void databasePropertiesReturnsNullWhenUrlDoesNotMatchPrefix() throws SQLException {
    V1alpha1Database venice = db("venice", "jdbc:venice://clusters=venice-cluster0", null, "VENICE");
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(apiReturning(venice));

    // Database exists for VENICE but the requested prefix is a different store type.
    assertThat(resolver.databaseProperties(null, "VENICE", "jdbc:kafka://")).isNull();
  }

  @Test
  void databasePropertiesReturnsNullWhenNoDatabaseMatches() throws SQLException {
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(apiReturning());

    assertThat(resolver.databaseProperties(null, "MISSING", "jdbc:kafka://")).isNull();
  }

  @Test
  void databaseNameReturnsCrdNameForSchemaStyle() throws SQLException {
    V1alpha1Database kafka = db("kafka-database",
        "jdbc:kafka://bootstrap.servers=localhost:9092", null, "KAFKA");
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(apiReturning(kafka));

    assertThat(resolver.databaseName(Arrays.asList("KAFKA", "my_topic"))).isEqualTo("kafka-database");
  }

  @Test
  void databaseNameMatchesCatalogStyleByCatalog() throws SQLException {
    V1alpha1Database mysql = db("mysql", "jdbc:mysql-hoptimator://url=jdbc:mysql://localhost:3306",
        "MYSQL", null);
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(apiReturning(mysql));

    // Catalog-style: three-segment path [CATALOG, SCHEMA, TABLE] matches on the catalog CRD.
    assertThat(resolver.databaseName(Arrays.asList("MYSQL", "test_database", "orders")))
        .isEqualTo("mysql");
  }

  @Test
  void databaseNameThrowsWhenNoDatabaseRegistered() throws SQLException {
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(apiReturning());

    assertThatThrownBy(() -> resolver.databaseName(Arrays.asList("UNKNOWN", "t")))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("No Database is registered");
  }

  @Test
  @SuppressWarnings("unchecked")
  void databaseNameThrowsWhenApiListFails() throws SQLException {
    K8sApi<V1alpha1Database, V1alpha1DatabaseList> api = mock(K8sApi.class);
    doThrow(new SQLException("boom")).when(api).list();
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(api);

    // A failed list is swallowed by findDatabase (returns null), so databaseName reports no match.
    assertThatThrownBy(() -> resolver.databaseName(Arrays.asList("KAFKA", "t")))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("No Database is registered");
  }

  @Test
  void realApiPathBuildsContextAndPropagatesConnectionFailure() {
    // Uses the public constructor so api() creates a real K8sContext + K8sApi. There is no live
    // cluster, so list() fails to connect. NOTE: K8sApi.list() surfaces connection failures as an
    // IllegalStateException (not SQLException), which findDatabase's catch (SQLException) does not
    // swallow -- so it propagates. This exercises the non-injected api() branch without a backend.
    Properties props = new Properties();
    props.setProperty(K8sContext.NAMESPACE_KEY, "ns");
    props.setProperty(K8sContext.SERVER_KEY, "https://k8s.invalid:6443");
    props.setProperty(K8sContext.TOKEN_KEY, "token");
    K8sDatabaseConfigResolver resolver = new K8sDatabaseConfigResolver(props);

    assertThatThrownBy(() -> resolver.databaseName(Arrays.asList("KAFKA", "t")))
        .isInstanceOf(RuntimeException.class);
  }
}
