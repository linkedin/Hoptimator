package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sApiEndpointTest {

  private final K8sApiEndpoint<V1ConfigMap, V1ConfigMapList> endpoint =
      new K8sApiEndpoint<>("ConfigMap", "mygroup", "v1", "configmaps", false,
          V1ConfigMap.class, V1ConfigMapList.class);

  @Test
  void kindReturnsKind() {
    assertEquals("ConfigMap", endpoint.kind());
  }

  @Test
  void groupReturnsGroup() {
    assertEquals("mygroup", endpoint.group());
  }

  @Test
  void versionReturnsVersion() {
    assertEquals("v1", endpoint.version());
  }

  @Test
  void apiVersionCombinesGroupAndVersion() {
    assertEquals("mygroup/v1", endpoint.apiVersion());
  }

  @Test
  void pluralReturnsPlural() {
    assertEquals("configmaps", endpoint.plural());
  }

  @Test
  void clusterScopedReturnsFalse() {
    assertFalse(endpoint.clusterScoped());
  }

  @Test
  void clusterScopedReturnsTrue() {
    K8sApiEndpoint<V1ConfigMap, V1ConfigMapList> clusterEndpoint =
        new K8sApiEndpoint<>("Namespace", "", "v1", "namespaces", true,
            V1ConfigMap.class, V1ConfigMapList.class);
    assertTrue(clusterEndpoint.clusterScoped());
  }

  @Test
  void elementTypeReturnsCorrectClass() {
    assertEquals(V1ConfigMap.class, endpoint.elementType());
  }

  @Test
  void listTypeReturnsCorrectClass() {
    assertEquals(V1ConfigMapList.class, endpoint.listType());
  }

  @Test
  void apiEndpointsConstants() {
    assertEquals("Pipeline", K8sApiEndpoints.PIPELINES.kind());
    assertEquals("hoptimator.linkedin.com", K8sApiEndpoints.PIPELINES.group());
    assertEquals("v1alpha1", K8sApiEndpoints.PIPELINES.version());
    assertEquals("pipelines", K8sApiEndpoints.PIPELINES.plural());
    assertFalse(K8sApiEndpoints.PIPELINES.clusterScoped());

    assertTrue(K8sApiEndpoints.NAMESPACES.clusterScoped());
    assertFalse(K8sApiEndpoints.SECRETS.clusterScoped());
  }
}
