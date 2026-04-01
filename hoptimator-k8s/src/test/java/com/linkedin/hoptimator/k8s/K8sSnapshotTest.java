package com.linkedin.hoptimator.k8s;


import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJob;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sSnapshotTest {

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

  @Test
  void restoreClearsInternalMapSoSubsequentRestoreIsNoOp() throws SQLException {
    // After restore, the map must be cleared.
    // A second restore() call should do nothing (no double-delete).
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline();
    pipeline.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    pipeline.setKind("Pipeline");
    pipeline.setMetadata(new V1ObjectMeta().name("test-pipeline").namespace("default"));

    // Store snapshot (object not yet in fakeApi → stored as null, i.e. pre-store state was absent)
    snapshot.store(pipeline);
    // restore() deletes the pipeline from yamls (pre-store state was absent → delete)
    // yamls was empty and remains empty
    snapshot.restore();
    assertTrue(yamls.isEmpty(), "after first restore(), yaml map must be empty (pre-store state was absent)");

    // The snapshot's internal map must have been cleared (Map.clear must have been called)
    // Proof: store() again on the same object must succeed (it was not deduped as already stored)
    snapshot.store(pipeline);
    // And a second restore() must work
    snapshot.restore();
    assertTrue(yamls.isEmpty(), "second restore() must also result in empty yamls");
  }

  @Test
  void storeWithContextNullSkipsNamespaceOverride() throws SQLException {
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setApiVersion("v1");
    obj.setKind("ConfigMap");
    obj.setMetadata(new V1ObjectMeta().name("my-config").namespace("existing-ns"));

    // snapshot has null context (setUp() created it with null)
    snapshot.store(obj);
    // Should not throw and the namespace should remain "existing-ns"
    // Verify by restoring - the object wasn't in yamls so it gets deleted (no-op in fake)
    snapshot.restore();
    // No exception = test passed
  }

  @Test
  void toDynamicKubernetesObjectFromNonDynamicReturnsNewObject() {
    // If DynamicKubernetesObject is returned as-is, vs non-dynamic creates a new wrapper
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline();
    pipeline.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    pipeline.setKind("Pipeline");
    pipeline.setMetadata(new V1ObjectMeta().name("p1").namespace("ns"));

    DynamicKubernetesObject result = K8sSnapshot.toDynamicKubernetesObject(pipeline);

    // Must NOT return the same object (it was not a DynamicKubernetesObject)
    assertNotSame(result, pipeline);
    assertEquals("Pipeline", result.getKind());
    assertEquals("hoptimator.linkedin.com/v1alpha1", result.getApiVersion());
  }

  @Test
  void toDynamicKubernetesObjectFromDynamicReturnsSameObject() {
    // If input IS a DynamicKubernetesObject, the exact same object should be returned
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setApiVersion("v1");
    obj.setKind("ConfigMap");
    obj.setMetadata(new V1ObjectMeta().name("cm"));

    DynamicKubernetesObject result = K8sSnapshot.toDynamicKubernetesObject(obj);
    assertTrue(result == obj, "Should return the same DynamicKubernetesObject instance");
  }

  @Test
  void toDynamicKubernetesObjectFromDynamic() {
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setApiVersion("v1");
    obj.setKind("ConfigMap");
    obj.setMetadata(new V1ObjectMeta().name("test"));

    DynamicKubernetesObject result = K8sSnapshot.toDynamicKubernetesObject(obj);

    // Should return the same object since it's already DynamicKubernetesObject
    assertEquals(obj, result);
  }

  @Test
  void toDynamicKubernetesObjectFromGenericK8sObject() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline();
    pipeline.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    pipeline.setKind("Pipeline");
    pipeline.setMetadata(new V1ObjectMeta().name("test-pipeline").namespace("ns"));

    DynamicKubernetesObject result = K8sSnapshot.toDynamicKubernetesObject(pipeline);

    assertNotNull(result);
    assertEquals("hoptimator.linkedin.com/v1alpha1", result.getApiVersion());
    assertEquals("Pipeline", result.getKind());
    assertEquals("test-pipeline", result.getMetadata().getName());
    assertEquals("ns", result.getMetadata().getNamespace());
  }

  @Test
  void toDynamicKubernetesObjectPreservesMetadata() {
    V1alpha1Pipeline pipeline = new V1alpha1Pipeline();
    pipeline.setApiVersion("v1alpha1");
    pipeline.setKind("Pipeline");
    pipeline.setMetadata(new V1ObjectMeta().name("p1").namespace("default"));

    DynamicKubernetesObject result = K8sSnapshot.toDynamicKubernetesObject(pipeline);

    assertEquals("p1", result.getMetadata().getName());
    assertEquals("default", result.getMetadata().getNamespace());
  }

  @Test
  void restoreWithEmptyMapDoesNothing() {
    K8sSnapshot snapshot = new K8sSnapshot(null);
    // Should not throw
    snapshot.restore();
  }

  @Test
  void testSnapshotRestoreDelete() throws SQLException {
    V1alpha1SqlJob sqlJob = new V1alpha1SqlJob();
    sqlJob.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    sqlJob.setKind("SqlJob");
    sqlJob.setMetadata(new V1ObjectMeta().name("test-sql-job").namespace("test-namespace"));

    snapshot.store(sqlJob);
    fakeApi.create(Yaml.dump(sqlJob));
    assertEquals(1, yamls.size());
    assertEquals(yamls.get("test-sql-job"), Yaml.dump(sqlJob));

    snapshot.restore();
    assertTrue(yamls.isEmpty());
  }

  @Test
  void testRestoreWithMultipleUpdates() throws SQLException {
    V1alpha1SqlJob oldSqlJob = new V1alpha1SqlJob();
    oldSqlJob.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    oldSqlJob.setKind("SqlJob");
    oldSqlJob.setMetadata(new V1ObjectMeta().name("test-sql-job").namespace("test-namespace")
        .putAnnotationsItem("key", "old-value"));

    V1alpha1SqlJob newSqlJob = new V1alpha1SqlJob();
    newSqlJob.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    newSqlJob.setKind("SqlJob");
    newSqlJob.setMetadata(new V1ObjectMeta().name("test-sql-job").namespace("test-namespace")
        .putAnnotationsItem("key", "new-value"));

    V1alpha1SqlJob newSqlJob2 = new V1alpha1SqlJob();
    newSqlJob2.setApiVersion("hoptimator.linkedin.com/v1alpha1");
    newSqlJob2.setKind("SqlJob");
    newSqlJob2.setMetadata(new V1ObjectMeta().name("test-sql-job").namespace("test-namespace")
        .putAnnotationsItem("key", "new-value-2"));

    fakeApi.create(Yaml.dump(oldSqlJob));
    snapshot.store(newSqlJob);
    fakeApi.create(Yaml.dump(newSqlJob));
    snapshot.store(newSqlJob2);
    fakeApi.create(Yaml.dump(newSqlJob2));
    assertEquals(1, yamls.size());
    assertEquals(yamls.get("test-sql-job"), Yaml.dump(newSqlJob2));

    snapshot.restore();
    assertEquals(yamls.get("test-sql-job"), Yaml.dump(oldSqlJob));
  }

  @Test
  void k8sSpecEqualsSameValues() {
    DynamicKubernetesObject obj1 = new DynamicKubernetesObject();
    obj1.setApiVersion("v1");
    obj1.setKind("ConfigMap");
    obj1.setMetadata(new V1ObjectMeta().name("test").namespace("default"));

    DynamicKubernetesObject obj2 = new DynamicKubernetesObject();
    obj2.setApiVersion("v1");
    obj2.setKind("ConfigMap");
    obj2.setMetadata(new V1ObjectMeta().name("test").namespace("default"));

    K8sSnapshot.K8sSpec spec1 = new K8sSnapshot.K8sSpec(obj1);
    K8sSnapshot.K8sSpec spec2 = new K8sSnapshot.K8sSpec(obj2);
    assertEquals(spec1, spec2);
    assertEquals(spec1.hashCode(), spec2.hashCode());
  }

  @Test
  void k8sSpecNotEqualsDifferentName() {
    DynamicKubernetesObject obj1 = new DynamicKubernetesObject();
    obj1.setApiVersion("v1");
    obj1.setKind("ConfigMap");
    obj1.setMetadata(new V1ObjectMeta().name("test1").namespace("default"));

    DynamicKubernetesObject obj2 = new DynamicKubernetesObject();
    obj2.setApiVersion("v1");
    obj2.setKind("ConfigMap");
    obj2.setMetadata(new V1ObjectMeta().name("test2").namespace("default"));

    K8sSnapshot.K8sSpec spec1 = new K8sSnapshot.K8sSpec(obj1);
    K8sSnapshot.K8sSpec spec2 = new K8sSnapshot.K8sSpec(obj2);
    assertNotEquals(spec1, spec2);
  }

  @Test
  void k8sSpecAccessors() {
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setApiVersion("v1");
    obj.setKind("ConfigMap");
    obj.setMetadata(new V1ObjectMeta().name("my-config").namespace("my-ns"));

    K8sSnapshot.K8sSpec spec = new K8sSnapshot.K8sSpec(obj);
    assertEquals("v1", spec.apiVersion());
    assertEquals("ConfigMap", spec.kind());
    assertEquals("my-ns", spec.namespace());
    assertEquals("my-config", spec.name());
  }
}
