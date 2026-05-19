package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.SqlJobDeployable;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJob;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJobList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sSqlJobDeployerTest {

  private List<V1alpha1SqlJob> objects;
  private FakeK8sApi<V1alpha1SqlJob, V1alpha1SqlJobList> fakeApi;
  private FakeK8sYamlApi fakeYamlApi;
  private K8sSnapshot snapshot;

  @BeforeEach
  void setUp() {
    objects = new ArrayList<>();
    fakeApi = new FakeK8sApi<>(objects);
    Map<String, String> yamls = new HashMap<>();
    fakeYamlApi = new FakeK8sYamlApi(yamls);
    snapshot = new K8sSnapshot(null) {
      @Override
      K8sYamlApi createYamlApi(K8sContext context) {
        return fakeYamlApi;
      }
    };
  }

  private K8sSqlJobDeployer makeDeployer(SqlJobDeployable sqlJob) {
    FakeK8sApi<V1alpha1SqlJob, V1alpha1SqlJobList> capturedApi = fakeApi;
    K8sSnapshot capturedSnapshot = snapshot;
    return new K8sSqlJobDeployer(sqlJob, null) {
      @Override
      K8sApi<V1alpha1SqlJob, V1alpha1SqlJobList> createApi(K8sContext context,
          K8sApiEndpoint<V1alpha1SqlJob, V1alpha1SqlJobList> endpoint) {
        return capturedApi;
      }

      @Override
      K8sSnapshot createSnapshot(K8sContext context) {
        return capturedSnapshot;
      }
    };
  }

  @Test
  void specifyWithSqlReturnsYaml() throws SQLException {
    SqlJobDeployable sqlJob = new SqlJobDeployable("my-job",
        null, null,
        Collections.singletonList("INSERT INTO sink SELECT * FROM source"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    assertTrue(specs.get(0).contains("my-job"));
    assertTrue(specs.get(0).contains("INSERT INTO sink SELECT * FROM source"));
  }

  @Test
  void specifyWithDialectAndExecutionModeSetsFields() throws SQLException {
    SqlJobDeployable sqlJob = new SqlJobDeployable("my-etl-job",
        "Flink", "Streaming",
        Collections.singletonList("INSERT INTO sink SELECT * FROM source"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    String yaml = specs.get(0);
    assertTrue(yaml.contains("my-etl-job"));
    assertTrue(yaml.contains("Flink"));
    assertTrue(yaml.contains("Streaming"));
  }

  @Test
  void specifyWithNoDialectOrExecutionModeOmitsOptionalFields() throws SQLException {
    SqlJobDeployable sqlJob = new SqlJobDeployable("bare-job",
        null, null,
        Collections.singletonList("SELECT 1"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    String yaml = specs.get(0);
    assertTrue(yaml.contains("bare-job"));
  }

  @Test
  void specifyWithOptionsPopulatesConfigs() throws SQLException {
    Map<String, String> options = new HashMap<>();
    options.put("checkpointing.interval", "10000");
    options.put("parallelism", "4");
    SqlJobDeployable sqlJob = new SqlJobDeployable("configured-job",
        "Flink", "Batch",
        Collections.singletonList("INSERT INTO sink SELECT * FROM source"),
        options);

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    String yaml = specs.get(0);
    assertTrue(yaml.contains("checkpointing.interval"));
    assertTrue(yaml.contains("10000"));
    assertTrue(yaml.contains("parallelism"));
    assertTrue(yaml.contains("4"));
  }

  @Test
  void specifyWithMultipleSqlStatementsIncludesAll() throws SQLException {
    SqlJobDeployable sqlJob = new SqlJobDeployable("multi-sql-job",
        "Flink", null,
        Arrays.asList("INSERT INTO sink1 SELECT * FROM source1",
            "INSERT INTO sink2 SELECT * FROM source2"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    String yaml = specs.get(0);
    assertTrue(yaml.contains("source1"));
    assertTrue(yaml.contains("source2"));
  }

  @Test
  void specifyYamlContainsCorrectApiVersionAndKind() throws SQLException {
    SqlJobDeployable sqlJob = new SqlJobDeployable("my-job",
        null, null,
        Collections.singletonList("INSERT INTO sink SELECT * FROM source"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    String yaml = specs.get(0);
    assertTrue(yaml.contains("SqlJob"), "spec must contain kind 'SqlJob'");
    assertTrue(yaml.contains("hoptimator.linkedin.com"), "spec must contain the Hoptimator API group");
  }

  @Test
  void createAddsObjectToApi() throws SQLException {
    SqlJobDeployable sqlJob = new SqlJobDeployable("my-job",
        "Flink", "Streaming",
        Collections.singletonList("INSERT INTO sink SELECT * FROM source"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);
    deployer.create();

    assertEquals(1, objects.size());
    assertEquals("my-job", objects.get(0).getMetadata().getName());
  }

  @Test
  void specifyNameIsCanonicalizedToLowerKebabCase() throws SQLException {
    SqlJobDeployable sqlJob = new SqlJobDeployable("MyEtlJob",
        null, null,
        Collections.singletonList("SELECT 1"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    assertTrue(specs.get(0).contains("myetljob"), "name must be canonicalized to lowercase");
  }

  @Test
  void specifyWithInvalidDialectThrowsIllegalArgumentException() {
    SqlJobDeployable sqlJob = new SqlJobDeployable("my-job",
        "InvalidDialect", null,
        Collections.singletonList("SELECT 1"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);

    assertThrows(IllegalArgumentException.class, deployer::specify);
  }

  @Test
  void specifyWithInvalidExecutionModeThrowsIllegalArgumentException() {
    SqlJobDeployable sqlJob = new SqlJobDeployable("my-job",
        "Flink", "InvalidMode",
        Collections.singletonList("SELECT 1"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);

    assertThrows(IllegalArgumentException.class, deployer::specify);
  }

  @Test
  void specifyWithBatchExecutionModeSetsBatch() throws SQLException {
    SqlJobDeployable sqlJob = new SqlJobDeployable("batch-job",
        "Flink", "Batch",
        Collections.singletonList("INSERT INTO sink SELECT * FROM source"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);
    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    String yaml = specs.get(0);
    assertTrue(yaml.contains("Batch"));
  }

  @Test
  void specifyReturnsNonNullSpec() throws SQLException {
    SqlJobDeployable sqlJob = new SqlJobDeployable("my-job",
        null, null,
        Collections.singletonList("SELECT 1"),
        Collections.emptyMap());

    K8sSqlJobDeployer deployer = makeDeployer(sqlJob);
    List<String> specs = deployer.specify();

    assertNotNull(specs);
    assertEquals(1, specs.size());
    assertNotNull(specs.get(0));
  }
}
