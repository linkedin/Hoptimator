package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;


/**
 * Resolution logic for the {@code !graph table} reverse-lookup CLI path. The bridge translates
 * SQL identifiers ({@code SCHEMA.TABLE} or {@code CATALOG.SCHEMA.TABLE}) into the K8s-side
 * canonical form Pipelines used to stamp their {@code depends-on-<slug>} labels (Database CRD
 * name + qualified path).
 */
@ExtendWith(MockitoExtension.class)
class K8sGraphProviderTest {

  @Mock
  private K8sApi<V1alpha1Database, V1alpha1DatabaseList> databaseApi;

  private static V1alpha1Database db(String name, String schema) {
    return db(name, /*catalog=*/null, schema);
  }

  private static V1alpha1Database db(String name, String catalog, String schema) {
    V1alpha1Database d = new V1alpha1Database();
    d.metadata(new V1ObjectMeta().name(name));
    V1alpha1DatabaseSpec spec = new V1alpha1DatabaseSpec();
    spec.setCatalog(catalog);
    spec.setSchema(schema);
    d.setSpec(spec);
    return d;
  }

  @Test
  void schemaMatchSubstitutesDatabaseAndPrependsSchema() throws SQLException {
    when(databaseApi.list()).thenReturn(Collections.singletonList(db("ads-database", "ADS")));

    K8sGraphProvider.Resolved out = K8sGraphProvider.resolveResource(
        databaseApi, "ADS", Collections.singletonList("AD_CLICKS"));

    assertEquals("ads-database", out.database);
    assertEquals(Arrays.asList("ADS", "AD_CLICKS"), out.path);
  }

  @Test
  void schemaMatchIsCaseInsensitiveButPreservesPathCase() throws SQLException {
    // Schema-name match is case-insensitive, but path tail is preserved verbatim. The bridge
    // can't tell whether a stamped label was upper- or mixed-case (Calcite-normalized MV sources
    // are upper; LogicalTable inter-tier pipelines are mixed). Users copy the canonical form
    // from !graph view / !graph logical output.
    when(databaseApi.list()).thenReturn(Collections.singletonList(db("ads-database", "ADS")));

    K8sGraphProvider.Resolved out = K8sGraphProvider.resolveResource(
        databaseApi, "ads", Collections.singletonList("ad_clicks"));

    assertEquals("ads-database", out.database);
    assertEquals(Arrays.asList("ADS", "ad_clicks"), out.path);
  }

  @Test
  void mixedCasePathSegmentPreservesItself() throws SQLException {
    // LogicalTable inter-tier pipelines stamp paths like [KAFKA, testevent] — schema upper from
    // the Database CRD, table preserved as the user wrote it. The bridge must not clobber the
    // tail case or the slug won't match.
    when(databaseApi.list()).thenReturn(Collections.singletonList(db("kafka-db", "KAFKA")));

    K8sGraphProvider.Resolved out = K8sGraphProvider.resolveResource(
        databaseApi, "KAFKA", Collections.singletonList("testevent"));

    assertEquals("kafka-db", out.database);
    assertEquals(Arrays.asList("KAFKA", "testevent"), out.path);
  }

  @Test
  void catalogMatchHandlesThreeLevelInput() throws SQLException {
    // User types `MYSQL.testdb.orders` — canonical 3-level form. Database CRD is named "mysql"
    // with catalog=MYSQL, schema=testdb. Bridge should substitute "mysql" as the database and
    // preserve the path verbatim (catalog already provides the first segment).
    when(databaseApi.list()).thenReturn(Collections.singletonList(db("mysql", "MYSQL", "testdb")));

    K8sGraphProvider.Resolved out = K8sGraphProvider.resolveResource(
        databaseApi, "MYSQL", Arrays.asList("testdb", "orders"));

    assertEquals("mysql", out.database);
    assertEquals(Arrays.asList("MYSQL", "testdb", "orders"), out.path);
  }

  @Test
  void catalogMatchInsertsSchemaWhenUserSkippedIt() throws SQLException {
    // User types `MYSQL.orders` — skipped the schema. Bridge inserts the schema so the slug
    // still matches `slug(mysql, [MYSQL, testdb, orders])`.
    when(databaseApi.list()).thenReturn(Collections.singletonList(db("mysql", "MYSQL", "testdb")));

    K8sGraphProvider.Resolved out = K8sGraphProvider.resolveResource(
        databaseApi, "MYSQL", Collections.singletonList("orders"));

    assertEquals("mysql", out.database);
    assertEquals(Arrays.asList("MYSQL", "testdb", "orders"), out.path);
  }

  @Test
  void schemaMatchPrependsCatalogWhenPresent() throws SQLException {
    // User types just `testdb.orders` (skipped the catalog). The Database has both catalog and
    // schema set, so the canonical path is [catalog, schema, ...rest].
    when(databaseApi.list()).thenReturn(Collections.singletonList(db("mysql", "MYSQL", "testdb")));

    K8sGraphProvider.Resolved out = K8sGraphProvider.resolveResource(
        databaseApi, "testdb", Collections.singletonList("orders"));

    assertEquals("mysql", out.database);
    assertEquals(Arrays.asList("MYSQL", "testdb", "orders"), out.path);
  }

  @Test
  void multipleDatabasesFirstSchemaMatchWins() throws SQLException {
    List<V1alpha1Database> all = Arrays.asList(
        db("profile-database", "PROFILE"),
        db("ads-database", "ADS"),
        db("nearline-database", "ADS"));   // duplicate schema (unrealistic, but tests determinism)
    when(databaseApi.list()).thenReturn(all);

    K8sGraphProvider.Resolved out = K8sGraphProvider.resolveResource(
        databaseApi, "ads", Collections.singletonList("ad_clicks"));

    // First match by iteration order.
    assertEquals("ads-database", out.database);
  }

  @Test
  void unknownSchemaReturnsNull() throws SQLException {
    // No catalog/schema matches → resolver returns null so the caller can pass the original
    // input through unchanged. The builder then produces the degenerate graph + warning.
    when(databaseApi.list()).thenReturn(Collections.emptyList());

    K8sGraphProvider.Resolved out = K8sGraphProvider.resolveResource(
        databaseApi, "UNKNOWN", Collections.singletonList("foo"));

    assertNull(out);
  }

  @Test
  void databaseWithoutSpecOrSchemaIsSkipped() throws SQLException {
    V1alpha1Database malformed = new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("malformed-db"));
    // spec intentionally null
    when(databaseApi.list()).thenReturn(Arrays.asList(malformed, db("ads-database", "ADS")));

    K8sGraphProvider.Resolved out = K8sGraphProvider.resolveResource(
        databaseApi, "ads", Collections.singletonList("ad_clicks"));

    // Malformed entry doesn't crash the resolution loop; the well-formed match still wins.
    assertEquals("ads-database", out.database);
  }
}
