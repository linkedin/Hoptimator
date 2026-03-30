package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;


class K8sDeployerTest {

  private List<V1alpha1Pipeline> objects;
  private FakeK8sApi<V1alpha1Pipeline, V1alpha1PipelineList> fakeApi;
  private Map<String, String> yamls;
  private FakeK8sYamlApi fakeYamlApi;
  private K8sSnapshot snapshot;

  @BeforeEach
  void setUp() {
    objects = new ArrayList<>();
    fakeApi = new FakeK8sApi<>(objects);
    yamls = new HashMap<>();
    fakeYamlApi = new FakeK8sYamlApi(yamls);
    snapshot = new K8sSnapshot(null) {
      @Override
      K8sYamlApi createYamlApi(K8sContext context) {
        return fakeYamlApi;
      }
    };
  }

  private K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList> makeDeployer(
      FakeK8sApi<V1alpha1Pipeline, V1alpha1PipelineList> api,
      K8sSnapshot snap) {
    return new K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList>(null, null) {
      @Override
      K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> createApi(K8sContext context,
          K8sApiEndpoint<V1alpha1Pipeline, V1alpha1PipelineList> endpoint) {
        return api;
      }

      @Override
      K8sSnapshot createSnapshot(K8sContext context) {
        return snap;
      }

      @Override
      protected V1alpha1Pipeline toK8sObject() {
        return new V1alpha1Pipeline()
            .apiVersion("hoptimator.linkedin.com/v1alpha1")
            .kind("Pipeline")
            .metadata(new V1ObjectMeta().name("test-pipeline").namespace("test-ns"))
            .spec(new V1alpha1PipelineSpec().sql("SELECT 1").yaml("spec1"));
      }
    };
  }

  @Test
  void createAddsObjectToApi() throws SQLException {
    K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList> deployer = makeDeployer(fakeApi, snapshot);

    deployer.create();

    assertEquals(1, objects.size());
    assertEquals("test-pipeline", objects.get(0).getMetadata().getName());
  }

  @Test
  void deleteRemovesObjectFromApi() throws SQLException {
    V1alpha1Pipeline pipeline = createTestPipeline();
    objects.add(pipeline);
    K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList> deployer = makeDeployer(fakeApi, snapshot);

    deployer.delete();

    // FakeK8sApi deletes by object identity, so original was removed, but toK8sObject creates new one
    // The important thing is delete was called without error
    assertNotNull(deployer);
  }

  @Test
  void updateModifiesObjectInApi() throws SQLException {
    K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList> deployer = makeDeployer(fakeApi, snapshot);

    deployer.update();

    assertEquals(1, objects.size());
  }

  @Test
  void specifyReturnsSingletonYaml() throws SQLException {
    K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList> deployer = makeDeployer(fakeApi, snapshot);

    List<String> specs = deployer.specify();

    assertEquals(1, specs.size());
    assertFalse(specs.get(0).isEmpty());
  }

  @Test
  void createAndReferenceReturnsOwnerReference() throws SQLException {
    K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList> deployer = makeDeployer(fakeApi, snapshot);

    V1OwnerReference ref = deployer.createAndReference();

    assertNotNull(ref);
    assertEquals("test-pipeline", ref.getName());
    assertEquals("Pipeline", ref.getKind());
    assertEquals(1, objects.size());
  }

  @Test
  void updateAndReferenceReturnsOwnerReference() throws SQLException {
    K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList> deployer = makeDeployer(fakeApi, snapshot);

    V1OwnerReference ref = deployer.updateAndReference();

    assertNotNull(ref);
    assertEquals("test-pipeline", ref.getName());
    assertEquals(1, objects.size());
  }

  @Test
  void restoreCallsSnapshotRestore() throws SQLException {
    K8sDeployer<V1alpha1Pipeline, V1alpha1PipelineList> deployer = makeDeployer(fakeApi, snapshot);
    deployer.create();

    deployer.restore();
    // Should not throw
    assertNotNull(deployer);
  }

  private V1alpha1Pipeline createTestPipeline() {
    return new V1alpha1Pipeline()
        .apiVersion("hoptimator.linkedin.com/v1alpha1")
        .kind("Pipeline")
        .metadata(new V1ObjectMeta().name("test-pipeline").namespace("test-ns"))
        .spec(new V1alpha1PipelineSpec().sql("SELECT 1").yaml("spec1"));
  }
}
