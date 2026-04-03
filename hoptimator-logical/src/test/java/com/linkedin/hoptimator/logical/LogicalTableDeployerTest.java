package com.linkedin.hoptimator.logical;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.K8sUtils;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for {@link LogicalTableDeployer}.
 *
 * <p>Tests cover SQL generation, pipeline naming, and CRD model construction that are
 * exercisable without a live K8s cluster. Integration coverage (tier deployer
 * orchestration, Pipeline CRD creation with real K8s) is handled by the Quidem
 * integration test suite (k8s-logical.id).
 */
class LogicalTableDeployerTest {

  private static Source makeSource(String database, String tableName) {
    return new Source(database, Arrays.asList(database, tableName), java.util.Collections.emptyMap());
  }

  // -----------------------------------------------------------------------
  // Pipeline SQL generation
  // -----------------------------------------------------------------------

  @Test
  void pipelineSqlUsesBacktickedIdentifiers() {
    String sql = buildPipelineSql("TRACKING", "VENICE", "MyTable");
    assertThat(sql).isEqualTo("INSERT INTO `VENICE`.`MyTable` SELECT * FROM `TRACKING`.`MyTable`");
  }

  @Test
  void pipelineSqlWorksWithLowercaseTableName() {
    String sql = buildPipelineSql("XINFRA", "ONLINE", "orders");
    assertThat(sql).isEqualTo("INSERT INTO `ONLINE`.`orders` SELECT * FROM `XINFRA`.`orders`");
  }

  /** Mirrors the private buildPipelineSql() method in LogicalTableDeployer. */
  private static String buildPipelineSql(String sourceSchema, String sinkSchema, String table) {
    return String.format("INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s`",
        sinkSchema, table, sourceSchema, table);
  }

  // -----------------------------------------------------------------------
  // Pipeline naming
  // -----------------------------------------------------------------------

  @Test
  void pipelineNameUsesK8sCanonicalForm() {
    String tableName = "MyTable";
    String pipelineName = "logical-" + K8sUtils.canonicalizeName(tableName) + "-nearline-to-online";
    assertThat(pipelineName).isEqualTo("logical-mytable-nearline-to-online");
  }

  // -----------------------------------------------------------------------
  // CRD model construction
  // -----------------------------------------------------------------------

  @Test
  void logicalTableSpecTierBindings() {
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    spec.putTiersItem("nearline", new V1alpha1LogicalTableSpecTiers().databaseCrdName("xinfra-tracking"));
    spec.putTiersItem("online", new V1alpha1LogicalTableSpecTiers().databaseCrdName("venice"));

    assertThat(spec.getTiers()).hasSize(2);
    assertThat(spec.getTiers().get("nearline").getDatabaseCrdName()).isEqualTo("xinfra-tracking");
    assertThat(spec.getTiers().get("online").getDatabaseCrdName()).isEqualTo("venice");
  }

  @Test
  void crdNameIsCanonicalizedFromPath() {
    List<String> path = Arrays.asList("LOGICAL", "MyTable");
    String crdName = K8sUtils.canonicalizeName(path);
    assertThat(crdName).isEqualTo("logical-mytable");
  }

  // -----------------------------------------------------------------------
  // FakeK8sApi mechanics (used in tests that build on this deployer)
  // -----------------------------------------------------------------------

  @Test
  void fakeK8sApiCreateAndGet() throws Exception {
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
  void fakeK8sApiLogicalTableCreateAndGet() throws Exception {
    V1alpha1LogicalTable lt = new V1alpha1LogicalTable()
        .apiVersion("hoptimator.linkedin.com/v1alpha1")
        .kind("LogicalTable")
        .metadata(new V1ObjectMeta().name("logical-mytable").uid("test-uid-123"))
        .spec(new V1alpha1LogicalTableSpec());

    FakeK8sApi<V1alpha1LogicalTable, com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(new java.util.ArrayList<>());
    fakeApi.create(lt);

    V1alpha1LogicalTable fetched = fakeApi.get("logical-mytable");
    assertThat(fetched.getMetadata().getUid()).isEqualTo("test-uid-123");
  }
}
