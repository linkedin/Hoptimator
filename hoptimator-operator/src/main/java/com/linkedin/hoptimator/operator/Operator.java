package com.linkedin.hoptimator.operator;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** Single handle to all the clients required by all the controllers. */
public class Operator {
  private final ApiClient apiClient;
  private final SharedInformerFactory informerFactory;
  private final Map<String, ApiInfo<?, ?>> apiInfo = new HashMap<>();

  public Operator(ApiClient apiClient, SharedInformerFactory informerFactory) {
    this.apiClient = apiClient;
    this.informerFactory = informerFactory;
  }

  public Operator(ApiClient apiClient) {
    this(apiClient, new SharedInformerFactory(apiClient));
  }
  
  public <T extends KubernetesObject, L extends KubernetesListObject> void registerApi(String kind,
      String singular, String plural, String group, String version, Class<T> type, Class<L> list) {
    registerApi(new ApiInfo<T, L>(kind, singular, plural, group, version, type, list));
  }

  public <T extends KubernetesObject, L extends KubernetesListObject> void registerApi(
      ApiInfo<T, L> api) {
    this.apiInfo.put(api.groupVersionKind(), api);
    // side-effect: register shared informer
    informerFactory.sharedIndexInformerFor(api.generic(apiClient), api.type(), resyncPeriod().toMillis());
  }

  @SuppressWarnings("unchecked")
  public <T extends KubernetesObject, L extends KubernetesListObject> ApiInfo<T, L> apiInfo(
      String groupVersionKind) {
    if (!this.apiInfo.containsKey(groupVersionKind)) {
      throw new IllegalArgumentException("No API for '" + groupVersionKind + "' registered!");
    }
    return (ApiInfo<T, L>) this.apiInfo.get(groupVersionKind);
  }

  public <T extends KubernetesObject> SharedIndexInformer<T> informer(String groupVersionKind) {
    ApiInfo<T, ?> api = apiInfo(groupVersionKind);
    return informerFactory.getExistingSharedIndexInformer(api.type());
  }

  public SharedInformerFactory informerFactory() {
    return informerFactory;
  }

  @SuppressWarnings("unchecked")
  public <T extends KubernetesObject> T fetch(String groupVersionKind, String namespace,
      String name) { 
    return (T) lister(groupVersionKind).namespace(namespace).get(name);
  }

  @SuppressWarnings("unchecked")
  public <T extends KubernetesObject> Lister<T> lister(String groupVersionKind) {
    return new Lister<T>((Indexer<T>) informer(groupVersionKind).getIndexer());
  }

  public DynamicKubernetesApi apiFor(DynamicKubernetesObject obj) {
    String groupVersion = obj.getApiVersion(); 
    String kind = obj.getKind();
    return apiInfo(groupVersion + "/" + kind).dynamic(apiClient);
  }

  @SuppressWarnings("unchecked")
  public <T extends KubernetesObject, L extends KubernetesListObject> GenericKubernetesApi<T, L>
      apiFor(String groupVersionKind) {
    return (GenericKubernetesApi<T, L>) apiInfo(groupVersionKind).generic(apiClient);
  }

  public Duration failureRetryDuration() {
    return Duration.ofMinutes(5);
  }

  public Duration resyncPeriod() {
    return Duration.ofMinutes(1);
  }

  public static class ApiInfo<T extends KubernetesObject, L extends KubernetesListObject> {
    private final String kind;
    private final String singular;
    private final String plural;
    private final String group;
    private final String version;
    private final Class<T> type;
    private final Class<L> list;

    public ApiInfo(String kind, String singular, String plural, String group, String version, Class<T> type, Class<L> list) {
      this.kind = kind;
      this.singular = singular;
      this.plural = plural;
      this.group = group;
      this.version = version;
      this.type = type;
      this.list = list;
    }

    public GenericKubernetesApi<T, L> generic(ApiClient apiClient) {
      return new GenericKubernetesApi<T, L>(type(), list(), group(), version(), plural(), apiClient);
    }

    public DynamicKubernetesApi dynamic(ApiClient apiClient) {
      return new DynamicKubernetesApi(group(), version(), plural(), apiClient);
    }

    public String kind() {
      return kind;
    }

    public String singular() {
      return singular;
    }

    public String plural() {
      return plural;
    }

    public String group() {
      return group;
    }

    public String version() {
      return version;
    }

    public String groupVersionKind() {
      return group() + "/" + version() + "/" + kind;
    }

    public String apiVersion() {
      return group() + "/" + version();
    }

    public Class<T> type() {
      return type;
    }

    public Class<L> list() {
      return list;
    }
  }
}
