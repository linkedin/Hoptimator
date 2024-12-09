package com.linkedin.hoptimator.k8s;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;


public class K8sApiEndpoint<T extends KubernetesObject, U extends KubernetesListObject> {

  private final String kind;
  private final String group;
  private final String version;
  private final String plural;
  private final boolean clusterScoped;
  private final Class<T> t;
  private final Class<U> u;
  private final String apiVersion;

  public K8sApiEndpoint(String kind, String group, String version, String plural, boolean clusterScoped, Class<T> t,
      Class<U> u) {
    this.kind = kind;
    this.group = group;
    this.version = version;
    this.plural = plural;
    this.clusterScoped = clusterScoped;
    this.t = t;
    this.u = u;
    this.apiVersion = String.join("/", group, version);
  }

  public String kind() {
    return kind;
  }

  public String group() {
    return group;
  }

  public String version() {
    return version;
  }

  public String apiVersion() {
    return apiVersion;
  }

  public String plural() {
    return plural;
  }

  /**
   * Most APIs are namespaced, but some are cluster-scoped.
   *
   * (e.g. Namespaces themselves are cluster-scoped.)
   *
   * */
  public boolean clusterScoped() {
    return clusterScoped;
  }

  public Class<T> elementType() {
    return t;
  }

  public Class<U> listType() {
    return u;
  }
}
