package com.linkedin.hoptimator.k8s;


import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sSnapshotTest {

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
  void k8sSpecEqualsNull() {
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setApiVersion("v1");
    obj.setKind("ConfigMap");
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("default"));

    K8sSnapshot.K8sSpec spec = new K8sSnapshot.K8sSpec(obj);
    assertFalse(spec.equals(null));
  }

  @Test
  void k8sSpecEqualsDifferentType() {
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setApiVersion("v1");
    obj.setKind("ConfigMap");
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("default"));

    K8sSnapshot.K8sSpec spec = new K8sSnapshot.K8sSpec(obj);
    assertFalse(spec.equals("not a K8sSpec"));
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
    assertTrue(spec1.equals(spec2));
    assertEquals(spec1.hashCode(), spec2.hashCode());
  }

  @Test
  void k8sSpecEqualsSameInstance() {
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setApiVersion("v1");
    obj.setKind("ConfigMap");
    obj.setMetadata(new V1ObjectMeta().name("test").namespace("default"));

    K8sSnapshot.K8sSpec spec = new K8sSnapshot.K8sSpec(obj);
    assertTrue(spec.equals(spec));
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
