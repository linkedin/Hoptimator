package com.linkedin.hoptimator.operator;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;

import com.linkedin.hoptimator.catalog.Resource;

import java.util.ArrayList;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Single handle to all the clients and configs required by all the controllers. */
public class Operator {
  private final static Logger log = LoggerFactory.getLogger(Operator.class);
 
  private final String namespace; 
  private final ApiClient apiClient;
  private final SharedInformerFactory informerFactory;
  private final Map<String, ApiInfo<?, ?>> apiInfo = new HashMap<>();
  private final Properties properties;

  public Operator(String namespace, ApiClient apiClient, SharedInformerFactory informerFactory,
      Properties properties) {
    this.namespace = namespace;
    this.apiClient = apiClient;
    this.informerFactory = informerFactory;
    this.properties = properties;
  }

  public Operator(String namespace, ApiClient apiClient, SharedInformerFactory informerFactory) {
    this(namespace, apiClient, informerFactory, new Properties());
  }

  public Operator(String namespace, ApiClient apiClient, Properties properties) {
    this(namespace, apiClient, new SharedInformerFactory(apiClient), properties);
  }

  public Operator(String namespace, ApiClient apiClient) {
    this(namespace, apiClient, new SharedInformerFactory(apiClient), new Properties());
  }

  /** Arbitrary global properties, which a controller plugin may use. */
  public Properties properties() {
    return properties;
  }
  
  public <T extends KubernetesObject, L extends KubernetesListObject> void registerApi(String kind,
      String singular, String plural, String group, String version, Class<T> type, Class<L> list) {
    registerApi(new ApiInfo<T, L>(kind, singular, plural, group, version, type, list), true);
  }

  public void registerApi(String kind, String singular, String plural, String group,
      String version) {
    registerApi(new ApiInfo<KubernetesObject, KubernetesListObject>(kind, singular, plural, group,
      version, KubernetesObject.class, KubernetesListObject.class), false);
  }

  public <T extends KubernetesObject, L extends KubernetesListObject> void registerApi(
      ApiInfo<T, L> api, boolean watch) {
    this.apiInfo.put(api.groupVersionKind(), api);
    if (watch) {
      // side-effect: register shared informer
      informerFactory.sharedIndexInformerFor(api.generic(apiClient), api.type(), resyncPeriod().toMillis(), namespace);
    }
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
    return apiFor(obj.getApiVersion(), obj.getKind());
  }

  public DynamicKubernetesApi apiFor(String apiVersion, String kind) {
    return apiInfo(apiVersion + "/" + kind).dynamic(apiClient);
  }

  @SuppressWarnings("unchecked")
  public <T extends KubernetesObject, L extends KubernetesListObject> GenericKubernetesApi<T, L>
      apiFor(String groupVersionKind) {
    return (GenericKubernetesApi<T, L>) apiInfo(groupVersionKind).generic(apiClient);
  }

  public DynamicKubernetesObject applyResource(Resource resource, V1OwnerReference ownerReference,
      Resource.TemplateFactory templateFactory) {
    String yaml = resource.render(templateFactory);
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    String namespace = obj.getMetadata().getNamespace();
    String name = obj.getMetadata().getName();
    String kind = obj.getKind();
    KubernetesApiResponse<DynamicKubernetesObject> existing = apiFor(obj).get(namespace, name);
    if (existing.isSuccess()) {
      String resourceVersion = existing.getObject().getMetadata().getResourceVersion();
      log.info("Updating existing downstream resource {}/{}/{} {} as \n{}",
        namespace, kind, name, resourceVersion, yaml);
      List<V1OwnerReference> owners = existing.getObject().getMetadata().getOwnerReferences();
      if (owners == null) {
        owners = new ArrayList<>();
      }
      if (owners.stream().anyMatch(x -> x.getUid().equals(ownerReference.getUid()))) {
        log.info("Existing downstream resource {}/{}/{} is already owned by {}/{}/{}.",
          namespace, kind, name, namespace, ownerReference.getKind(), ownerReference.getName());
      } else {
        log.info("Existing downstream resource {}/{}/{} will be owned by {}/{}/{} and {} others.",
          namespace, kind, name, namespace, ownerReference.getKind(), ownerReference.getName(),
          owners.size());
        owners.add(ownerReference);
      }
      obj.setMetadata(obj.getMetadata().ownerReferences(owners).resourceVersion(resourceVersion));
      KubernetesApiResponse<DynamicKubernetesObject> updated = apiFor(obj).update(obj);
      if (!updated.isSuccess()) {
        log.error("Error updating downstream resource {}/{}/{}: {}.", namespace, kind, name,
          updated.getStatus().getMessage());
        return existing.getObject();
      } else {
        return updated.getObject();
      }
    } else {
      log.info("Creating downstream resource {}/{}/{} as \n{}", namespace, kind, name, yaml);
      obj.setMetadata(obj.getMetadata().addOwnerReferencesItem(ownerReference));
      KubernetesApiResponse<DynamicKubernetesObject> created = apiFor(obj).create(obj);
      if (!created.isSuccess()) {
        log.error("Error creating downstream resource {}/{}/{}: {}.", namespace, kind, name,
          created.getStatus().getMessage());
        return null;
      } else {
        return created.getObject();
      }
    }
  }
   
  public Duration failureRetryDuration() {
    return Duration.ofMinutes(5);
  }

  public Duration pendingRetryDuration() {
    return Duration.ofSeconds(5);
  }

  public Duration resyncPeriod() {
    return Duration.ofMinutes(10);
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
