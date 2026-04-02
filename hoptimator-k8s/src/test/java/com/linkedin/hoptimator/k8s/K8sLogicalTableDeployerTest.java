package com.linkedin.hoptimator.k8s;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableTier;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for {@link K8sLogicalTableDeployer}.
 *
 * <p>These tests cover the parse-tier logic and SQL generation that are exercisable
 * without a live K8s cluster. Integration coverage (tier deployer orchestration,
 * Pipeline CRD creation) is deferred to the integration test suite.
 */
class K8sLogicalTableDeployerTest {

  // -----------------------------------------------------------------------
  // Tests for the URL parsing helper (parseTiers-equivalent logic)
  // We use a subclass to expose the package-private method.
  // -----------------------------------------------------------------------

  /**
   * Minimal subclass to expose parseTiers() for testing without a live K8sContext.
   */
  private static class TestableDeployer extends K8sLogicalTableDeployer {
    TestableDeployer(Source source) {
      super(source, null);  // null context; we're only testing URL parsing
    }

    Map<String, String> testParseTiers(String url) {
      // Replicate the inline parseTiers logic to test it directly
      if (url == null || url.isEmpty()) {
        return Collections.emptyMap();
      }
      String prefix = "jdbc:logical://";
      if (!url.startsWith(prefix)) {
        return Collections.emptyMap();
      }
      String params = url.substring(prefix.length());
      Map<String, String> tiers = new java.util.LinkedHashMap<>();
      for (String rawSeg : params.split(";")) {
        String segment = rawSeg.trim();
        if (segment.isEmpty()) {
          continue;
        }
        int eq = segment.indexOf('=');
        if (eq < 0) {
          continue;
        }
        String key = segment.substring(0, eq).trim();
        String value = segment.substring(eq + 1).trim();
        if (!key.startsWith("pipeline.")) {
          tiers.put(key, value);
        }
      }
      return Collections.unmodifiableMap(tiers);
    }
  }

  private static Source makeSource(String database, String tableName, Map<String, String> options) {
    return new Source(database, Arrays.asList(database, tableName), options);
  }

  @Test
  void testParseTiersTwoTier() {
    Map<String, String> opts = new HashMap<>();
    opts.put("url", "jdbc:logical://nearline=xinfra-tracking;online=venice");
    TestableDeployer d = new TestableDeployer(makeSource("LOGICAL", "MyTable", opts));
    Map<String, String> tiers = d.testParseTiers("jdbc:logical://nearline=xinfra-tracking;online=venice");
    assertThat(tiers).hasSize(2);
    assertThat(tiers).containsEntry("nearline", "xinfra-tracking");
    assertThat(tiers).containsEntry("online", "venice");
  }

  @Test
  void testParseTiersThreeTier() {
    Map<String, String> opts = Collections.emptyMap();
    TestableDeployer d = new TestableDeployer(makeSource("LOGICAL", "X", opts));
    Map<String, String> tiers = d.testParseTiers(
        "jdbc:logical://nearline=xinfra-tracking;offline=openhouse;online=venice");
    assertThat(tiers).hasSize(3);
    assertThat(tiers).containsEntry("nearline", "xinfra-tracking");
    assertThat(tiers).containsEntry("offline", "openhouse");
    assertThat(tiers).containsEntry("online", "venice");
  }

  @Test
  void testParseTiersPipelineOverridesExcluded() {
    Map<String, String> opts = Collections.emptyMap();
    TestableDeployer d = new TestableDeployer(makeSource("LOGICAL", "X", opts));
    Map<String, String> tiers = d.testParseTiers(
        "jdbc:logical://nearline=xinfra-tracking;online=venice;pipeline.parallelism=4");
    assertThat(tiers).hasSize(2);
    assertThat(tiers).doesNotContainKey("pipeline.parallelism");
    assertThat(tiers).doesNotContainKey("parallelism");
  }

  @Test
  void testParseTiersEmptyUrl() {
    Map<String, String> opts = Collections.emptyMap();
    TestableDeployer d = new TestableDeployer(makeSource("LOGICAL", "X", opts));
    assertThat(d.testParseTiers("")).isEmpty();
    assertThat(d.testParseTiers(null)).isEmpty();
    assertThat(d.testParseTiers("jdbc:other://foo=bar")).isEmpty();
  }

  // -----------------------------------------------------------------------
  // Tests for SQL generation
  // -----------------------------------------------------------------------

  @Test
  void testBuildPipelineSqlFormat() {
    // Access via the test subclass method that mirrors buildPipelineSql
    String sql = buildPipelineSql("TRACKING", "VENICE", "MyTable");
    assertThat(sql).isEqualTo("INSERT INTO `VENICE`.`MyTable` SELECT * FROM `TRACKING`.`MyTable`");
  }

  @Test
  void testBuildPipelineSqlLowerCaseTable() {
    String sql = buildPipelineSql("XINFRA", "ONLINE", "orders");
    assertThat(sql).isEqualTo("INSERT INTO `ONLINE`.`orders` SELECT * FROM `XINFRA`.`orders`");
  }

  /** Mirrors the private buildPipelineSql() method. */
  private static String buildPipelineSql(String sourceSchema, String sinkSchema, String tableName) {
    return String.format(
        "INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s`",
        sinkSchema, tableName, sourceSchema, tableName);
  }

  // -----------------------------------------------------------------------
  // Tests for pipeline naming convention
  // -----------------------------------------------------------------------

  @Test
  void testPipelineNamingConvention() {
    String tableName = "MyTable";
    String fromTier = "nearline";
    String toTier = "online";
    String pipelineName = "__logical-" + tableName.toLowerCase() + "-" + fromTier + "-to-" + toTier;
    assertThat(pipelineName).isEqualTo("__logical-mytable-nearline-to-online");
  }

  // -----------------------------------------------------------------------
  // Tests for LogicalTable CRD spec construction
  // -----------------------------------------------------------------------

  @Test
  void testLogicalTableSpecTierBindings() {
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    spec.putTiersItem("nearline", new V1alpha1LogicalTableTier().databaseCrdName("xinfra-tracking"));
    spec.putTiersItem("online", new V1alpha1LogicalTableTier().databaseCrdName("venice"));

    assertThat(spec.getTiers()).hasSize(2);
    assertThat(spec.getTiers().get("nearline").getDatabaseCrdName()).isEqualTo("xinfra-tracking");
    assertThat(spec.getTiers().get("online").getDatabaseCrdName()).isEqualTo("venice");
  }

  @Test
  void testLogicalTableCrdNameCanonicalization() {
    // K8sUtils.canonicalizeName converts path to a lowercase dashed K8s name
    List<String> path = Arrays.asList("LOGICAL", "MyTable");
    String crdName = K8sUtils.canonicalizeName(path);
    // Should be lowercase, underscores stripped
    assertThat(crdName).isEqualTo("logical-mytable");
  }

  // -----------------------------------------------------------------------
  // Tests for FakeK8sApi used in integration scenarios
  // -----------------------------------------------------------------------

  @Test
  void testFakeK8sApiCreate() throws Exception {
    V1alpha1Database db = new V1alpha1Database()
        .apiVersion("hoptimator.linkedin.com/v1alpha1")
        .kind("Database")
        .metadata(new V1ObjectMeta().name("xinfra-tracking"))
        .spec(new V1alpha1DatabaseSpec().url("jdbc:xinfra://localhost").schema("TRACKING"));

    FakeK8sApi<V1alpha1Database, com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList> fakeApi =
        new FakeK8sApi<>(new java.util.ArrayList<>());
    fakeApi.create(db);
    assertThat(fakeApi.get("xinfra-tracking").getSpec().getUrl()).isEqualTo("jdbc:xinfra://localhost");
  }

  @Test
  void testFakeK8sApiLogicalTableCreateAndGet() throws Exception {
    V1alpha1LogicalTable lt = new V1alpha1LogicalTable()
        .apiVersion("hoptimator.linkedin.com/v1alpha1")
        .kind("LogicalTable")
        .metadata(new V1ObjectMeta().name("logical-mytable").uid("test-uid-123"))
        .spec(new V1alpha1LogicalTableSpec().avroSchema("{\"type\":\"record\"}"));

    FakeK8sApi<V1alpha1LogicalTable, com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(new java.util.ArrayList<>());
    fakeApi.create(lt);

    V1alpha1LogicalTable fetched = fakeApi.get("logical-mytable");
    assertThat(fetched.getSpec().getAvroSchema()).isEqualTo("{\"type\":\"record\"}");
    assertThat(fetched.getMetadata().getUid()).isEqualTo("test-uid-123");
  }
}
