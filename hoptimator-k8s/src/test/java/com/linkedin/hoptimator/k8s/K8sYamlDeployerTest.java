package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sYamlDeployerTest {

  private static final String TEST_YAML = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: test-cm\n  namespace: default\n";

  private Map<String, String> yamls;
  private FakeK8sYamlApi fakeApi;
  private K8sSnapshot snapshot;

  @BeforeEach
  void setUp() {
    yamls = new HashMap<>();
    fakeApi = new FakeK8sYamlApi(yamls);
    snapshot = new K8sSnapshot(null) {
      @Override
      K8sYamlApi createYamlApi(K8sContext context) {
        return fakeApi;
      }
    };
  }

  private K8sYamlDeployerImpl makeDeployer(List<String> specs) {
    FakeK8sYamlApi capturedApi = fakeApi;
    K8sSnapshot capturedSnapshot = snapshot;
    return new K8sYamlDeployerImpl(null, specs) {
      @Override
      K8sYamlApi createYamlApi(K8sContext context) {
        return capturedApi;
      }

      @Override
      K8sSnapshot createSnapshot(K8sContext context) {
        return capturedSnapshot;
      }
    };
  }

  @Test
  void createAddsObjectsViaApi() throws SQLException {
    K8sYamlDeployerImpl deployer = makeDeployer(Arrays.asList(TEST_YAML));

    deployer.create();

    assertEquals(1, yamls.size());
    assertTrue(yamls.containsKey("test-cm"));
  }

  @Test
  void deleteRemovesObjectsViaApi() throws SQLException {
    // First create the object
    yamls.put("test-cm", TEST_YAML);
    K8sYamlDeployerImpl deployer = makeDeployer(Arrays.asList(TEST_YAML));

    deployer.delete();

    assertTrue(yamls.isEmpty());
  }

  @Test
  void updateModifiesObjectsViaApi() throws SQLException {
    K8sYamlDeployerImpl deployer = makeDeployer(Arrays.asList(TEST_YAML));

    deployer.update();

    assertEquals(1, yamls.size());
  }

  @Test
  void restoreCallsSnapshotRestore() throws SQLException {
    K8sYamlDeployerImpl deployer = makeDeployer(Arrays.asList(TEST_YAML));
    deployer.create();

    deployer.restore();

    // Snapshot should delete the object since it didn't exist before create
    assertTrue(yamls.isEmpty());
  }

  @Test
  void createWithMultipleSpecs() throws SQLException {
    String yaml2 = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: test-cm2\n  namespace: default\n";
    K8sYamlDeployerImpl deployer = makeDeployer(Arrays.asList(TEST_YAML, yaml2));

    deployer.create();

    assertEquals(2, yamls.size());
    assertTrue(yamls.containsKey("test-cm"));
    assertTrue(yamls.containsKey("test-cm2"));
  }

  @Test
  void specifyReturnsProvidedSpecs() throws SQLException {
    K8sYamlDeployerImpl deployer = makeDeployer(Arrays.asList(TEST_YAML));

    assertEquals(1, deployer.specify().size());
    assertEquals(TEST_YAML, deployer.specify().get(0));
  }
}
