package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1SecretList;

import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import com.linkedin.hoptimator.k8s.models.V1alpha1ViewList;


public final class K8sApiEndpoints {

  // Native K8s endpoints
  public static final K8sApiEndpoint<V1Namespace, V1NamespaceList> NAMESPACES =
      new K8sApiEndpoint<>("Namespace", "", "v1", "namespaces", true, V1Namespace.class, V1NamespaceList.class);
  public static final K8sApiEndpoint<V1Secret, V1SecretList> SECRETS =
      new K8sApiEndpoint<>("Secret", "", "v1", "secrets", false, V1Secret.class, V1SecretList.class);
  public static final K8sApiEndpoint<V1ConfigMap, V1ConfigMapList> CONFIG_MAPS =
      new K8sApiEndpoint<>("ConfigMap", "", "v1", "configmaps", false,
          V1ConfigMap.class, V1ConfigMapList.class);

  // Hoptimator custom resources
  public static final K8sApiEndpoint<V1alpha1Database, V1alpha1DatabaseList> DATABASES =
      new K8sApiEndpoint<>("Database", "hoptimator.linkedin.com", "v1alpha1", "databases", false,
          V1alpha1Database.class, V1alpha1DatabaseList.class);
  public static final K8sApiEndpoint<V1alpha1Pipeline, V1alpha1PipelineList> PIPELINES =
      new K8sApiEndpoint<>("Pipeline", "hoptimator.linkedin.com", "v1alpha1", "pipelines", false,
          V1alpha1Pipeline.class, V1alpha1PipelineList.class);
  public static final K8sApiEndpoint<V1alpha1View, V1alpha1ViewList> VIEWS =
      new K8sApiEndpoint<>("View", "hoptimator.linkedin.com", "v1alpha1", "views", false, V1alpha1View.class,
          V1alpha1ViewList.class);
  public static final K8sApiEndpoint<V1alpha1TableTemplate, V1alpha1TableTemplateList> TABLE_TEMPLATES =
      new K8sApiEndpoint<>("TableTemplate", "hoptimator.linkedin.com", "v1alpha1", "tabletemplates", false,
          V1alpha1TableTemplate.class, V1alpha1TableTemplateList.class);
  public static final K8sApiEndpoint<V1alpha1JobTemplate, V1alpha1JobTemplateList> JOB_TEMPLATES =
      new K8sApiEndpoint<>("JobTemplate", "hoptimator.linkedin.com", "v1alpha1", "jobtemplates", false,
          V1alpha1JobTemplate.class, V1alpha1JobTemplateList.class);

  private K8sApiEndpoints() {
  }
}
