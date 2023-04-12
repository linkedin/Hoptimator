package com.linkedin.hoptimator;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.util.generic.GenericKubernetesApi;

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
  
  public <T extends KubernetesObject, L extends KubernetesListObject> void registerApi(String singular,
      String plural, String group, String version, Class<T> type, Class<L> list) {
    registerApi(new ApiInfo<T, L>(singular, plural, group, version, type, list));
  }

  public <T extends KubernetesObject, L extends KubernetesListObject> void registerApi(ApiInfo<T, L> api) {
    this.apiInfo.put(api.singular(), api);
    // side-effect: register shared informer
    informerFactory.sharedIndexInformerFor(api.generic(apiClient), api.type(), resyncPeriod().toMillis());
  }

  public void start() {
    informerFactory.startAllRegisteredInformers();
  }

  public <T extends KubernetesObject, L extends KubernetesListObject> ApiInfo<T, L> apiInfo(String singular) {
    if (!this.apiInfo.containsKey(singular)) {
      throw new IllegalArgumentException("No API for '" + singular + "' registered!");
    }
    return (ApiInfo<T, L>) this.apiInfo.get(singular);
  }

  public <T extends KubernetesObject> SharedIndexInformer<T> informer(String singular) {
    ApiInfo<T, ?> api = apiInfo(singular);
    return informerFactory.getExistingSharedIndexInformer(api.type());
  }

  public SharedInformerFactory informerFactory() {
    return informerFactory;
  }

  public <T extends KubernetesObject> T fetch(String singular, String namespace, String name) {
    return (T) lister(singular).namespace(namespace).get(name);
  }

  public <T extends KubernetesObject> Lister<T> lister(String singular) {
    return new Lister<T>((Indexer<T>) informer(singular).getIndexer());
  }

  public Duration failureRetryDuration() {
    return Duration.ofMinutes(5);
  }

  public Duration resyncPeriod() {
    return Duration.ofMinutes(1);
  }

  public static class ApiInfo<T extends KubernetesObject, L extends KubernetesListObject> {
    private final String singular;
    private final String plural;
    private final String group;
    private final String version;
    private final Class<T> type;
    private final Class<L> list;

    public ApiInfo(String singular, String plural, String group, String version, Class<T> type, Class<L> list) {
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

    public Class<T> type() {
      return type;
    }

    public Class<L> list() {
      return list;
    }
  }
}
