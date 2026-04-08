package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class K8sApiEndpointsTest {

  @Test
  void namespacesEndpointIsClusterScoped() {
    assertTrue(K8sApiEndpoints.NAMESPACES.clusterScoped());
    assertEquals("Namespace", K8sApiEndpoints.NAMESPACES.kind());
    assertEquals("namespaces", K8sApiEndpoints.NAMESPACES.plural());
    assertEquals("v1", K8sApiEndpoints.NAMESPACES.version());
    assertEquals("", K8sApiEndpoints.NAMESPACES.group());
    assertEquals("/v1", K8sApiEndpoints.NAMESPACES.apiVersion());
    assertEquals(V1Namespace.class, K8sApiEndpoints.NAMESPACES.elementType());
    assertEquals(V1NamespaceList.class, K8sApiEndpoints.NAMESPACES.listType());
  }

  @Test
  void pipelinesEndpointIsNamespaceScoped() {
    assertFalse(K8sApiEndpoints.PIPELINES.clusterScoped());
    assertEquals("Pipeline", K8sApiEndpoints.PIPELINES.kind());
    assertEquals("pipelines", K8sApiEndpoints.PIPELINES.plural());
    assertEquals("hoptimator.linkedin.com", K8sApiEndpoints.PIPELINES.group());
    assertEquals("v1alpha1", K8sApiEndpoints.PIPELINES.version());
    assertEquals("hoptimator.linkedin.com/v1alpha1", K8sApiEndpoints.PIPELINES.apiVersion());
    assertEquals(V1alpha1Pipeline.class, K8sApiEndpoints.PIPELINES.elementType());
    assertEquals(V1alpha1PipelineList.class, K8sApiEndpoints.PIPELINES.listType());
  }

  @Test
  void secretsEndpointIsNamespaceScoped() {
    assertFalse(K8sApiEndpoints.SECRETS.clusterScoped());
    assertEquals("Secret", K8sApiEndpoints.SECRETS.kind());
    assertEquals("secrets", K8sApiEndpoints.SECRETS.plural());
  }

  @Test
  void configMapsEndpointIsNamespaceScoped() {
    assertFalse(K8sApiEndpoints.CONFIG_MAPS.clusterScoped());
    assertEquals("ConfigMap", K8sApiEndpoints.CONFIG_MAPS.kind());
    assertEquals("configmaps", K8sApiEndpoints.CONFIG_MAPS.plural());
  }

  @Test
  void databasesEndpointHasCorrectKind() {
    assertEquals("Database", K8sApiEndpoints.DATABASES.kind());
    assertEquals("databases", K8sApiEndpoints.DATABASES.plural());
  }

  @Test
  void enginesEndpointHasCorrectKind() {
    assertEquals("Engine", K8sApiEndpoints.ENGINES.kind());
    assertEquals("engines", K8sApiEndpoints.ENGINES.plural());
  }

  @Test
  void viewsEndpointHasCorrectKind() {
    assertEquals("View", K8sApiEndpoints.VIEWS.kind());
    assertEquals("views", K8sApiEndpoints.VIEWS.plural());
  }

  @Test
  void tableTemplatesEndpointHasCorrectKind() {
    assertEquals("TableTemplate", K8sApiEndpoints.TABLE_TEMPLATES.kind());
    assertEquals("tabletemplates", K8sApiEndpoints.TABLE_TEMPLATES.plural());
  }

  @Test
  void jobTemplatesEndpointHasCorrectKind() {
    assertEquals("JobTemplate", K8sApiEndpoints.JOB_TEMPLATES.kind());
    assertEquals("jobtemplates", K8sApiEndpoints.JOB_TEMPLATES.plural());
  }

  @Test
  void tableTriggersEndpointHasCorrectKind() {
    assertEquals("TableTrigger", K8sApiEndpoints.TABLE_TRIGGERS.kind());
    assertEquals("tabletriggers", K8sApiEndpoints.TABLE_TRIGGERS.plural());
  }

  @Test
  void jobsEndpointHasCorrectKind() {
    assertEquals("Job", K8sApiEndpoints.JOBS.kind());
    assertEquals("jobs", K8sApiEndpoints.JOBS.plural());
    assertEquals("batch", K8sApiEndpoints.JOBS.group());
  }
}
