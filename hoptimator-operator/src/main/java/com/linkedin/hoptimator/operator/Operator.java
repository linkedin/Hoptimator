package com.linkedin.hoptimator.operator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.informer.cache.Lister;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;


/** Single handle to all the clients and configs required by all the controllers. */
public class Operator {
  private final static Logger log = LoggerFactory.getLogger(Operator.class);

  private final String namespace;
  private final ApiClient apiClient;
  private final SharedInformerFactory informerFactory;
  private final Map<String, ApiInfo<?, ?>> apiInfo = new HashMap<>();
  private final Properties properties;

  public Operator(String namespace, ApiClient apiClient, SharedInformerFactory informerFactory, Properties properties) {
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

  public <T extends KubernetesObject, L extends KubernetesListObject> void registerApi(String kind, String singular,
      String plural, String group, String version, Class<T> type, Class<L> list) {
    registerApi(new ApiInfo<>(kind, singular, plural, group, version, type, list), true);
  }

  public void registerApi(String kind, String singular, String plural, String group, String version) {
    registerApi(new ApiInfo<>(kind, singular, plural, group, version, KubernetesObject.class, KubernetesListObject.class), false);
  }

  public <T extends KubernetesObject, L extends KubernetesListObject> void registerApi(ApiInfo<T, L> api,
      boolean watch) {
    this.apiInfo.put(api.groupVersionKind(), api);
    if (watch) {
      // side-effect: register shared informer
      informerFactory.sharedIndexInformerFor(api.generic(apiClient), api.type(), resyncPeriod().toMillis(), namespace);
    }
  }

  @SuppressWarnings("unchecked")
  public <T extends KubernetesObject, L extends KubernetesListObject> ApiInfo<T, L> apiInfo(String groupVersionKind) {
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
  public <T extends KubernetesObject> T fetch(String groupVersionKind, String namespace, String name) {
    return (T) lister(groupVersionKind).namespace(namespace).get(name);
  }

  @SuppressWarnings("unchecked")
  public <T extends KubernetesObject> Lister<T> lister(String groupVersionKind) {
    return new Lister<>((Indexer<T>) informer(groupVersionKind).getIndexer());
  }

  public DynamicKubernetesApi apiFor(DynamicKubernetesObject obj) {
    String groupVersion = obj.getApiVersion();
    String kind = obj.getKind();
    return apiInfo(groupVersion + "/" + kind).dynamic(apiClient);
  }

  @SuppressWarnings("unchecked")
  public <T extends KubernetesObject, L extends KubernetesListObject> GenericKubernetesApi<T, L> apiFor(
      String groupVersionKind) {
    return (GenericKubernetesApi<T, L>) apiInfo(groupVersionKind).generic(apiClient);
  }

  public Duration failureRetryDuration() {
    return Duration.ofMinutes(5);
  }

  public Duration pendingRetryDuration() {
    return Duration.ofMinutes(1);
  }

  public Duration resyncPeriod() {
    return Duration.ofMinutes(10);
  }

  public void apply(String yaml, KubernetesObject owner) throws ApiException {
    V1OwnerReference ownerReference = new V1OwnerReference().kind(owner.getKind())
        .name(owner.getMetadata().getName())
        .apiVersion(owner.getApiVersion())
        .uid(owner.getMetadata().getUid());
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    if (obj.getMetadata().getNamespace() == null) {
      obj.getMetadata().setNamespace(owner.getMetadata().getNamespace());
    }
    if (obj.getMetadata().getName() == null) {
      throw new IllegalArgumentException("Object has no name.");
    }
    String namespace = obj.getMetadata().getNamespace();
    String name = obj.getMetadata().getName();
    KubernetesApiResponse<DynamicKubernetesObject> existing = apiFor(obj).get(namespace, name);
    if (existing.isSuccess()) {
      String resourceVersion = existing.getObject().getMetadata().getResourceVersion();
      log.info("Updating existing downstream resource {}/{} {} as \n{}", namespace, name, resourceVersion, yaml);
      List<V1OwnerReference> owners = existing.getObject().getMetadata().getOwnerReferences();
      if (owners == null) {
        owners = new ArrayList<>();
      }
      if (owners.stream().anyMatch(x -> x.getUid().equals(ownerReference.getUid()))) {
        log.info("Existing downstream resource {}/{} is already owned by {}/{}.", namespace, name,
            ownerReference.getKind(), ownerReference.getName());
      } else {
        log.info("Existing downstream resource {}/{} will be owned by {}/{} and {} others.", namespace, name,
            ownerReference.getKind(), ownerReference.getName(), owners.size());
        owners.add(ownerReference);
      }
      obj.setMetadata(obj.getMetadata().ownerReferences(owners).resourceVersion(resourceVersion));
      apiFor(obj).update(obj)
          .onFailure(
              (x, y) -> log.error("Error updating downstream resource {}/{}: {}.", namespace, name, y.getMessage()))
          .throwsApiException();
    } else {
      log.info("Creating downstream resource {}/{} as \n{}", namespace, name, yaml);
      obj.setMetadata(obj.getMetadata().addOwnerReferencesItem(ownerReference));
      apiFor(obj).create(obj)
          .onFailure(
              (x, y) -> log.error("Error creating downstream resource {}/{}: {}.", namespace, name, y.getMessage()))
          .throwsApiException();
    }
  }

  public boolean isReady(String yaml) {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    String namespace = obj.getMetadata().getNamespace();
    String name = obj.getMetadata().getName();
    String kind = obj.getKind();
    try {
      KubernetesApiResponse<DynamicKubernetesObject> existing = apiFor(obj).get(namespace, name);
      existing.onFailure((code, status) -> log.warn("Failed to fetch {}/{}: {}.", kind, name, status.getMessage()));
      if (!existing.isSuccess()) {
        return false;
      }
      if (isReady(existing.getObject())) {
        log.info("{}/{} is ready.", kind, name);
        return true;
      } else {
        log.info("{}/{} is NOT ready.", kind, name);
        return false;
      }
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isReady(DynamicKubernetesObject obj) {
    // We make a best effort to guess the status of the dynamic object. By default, it's ready.
    if (obj == null || obj.getRaw() == null) {
      return false;
    }
    try {
      return obj.getRaw().get("status").getAsJsonObject().get("ready").getAsBoolean();
    } catch (Exception e) {
      log.debug("Exception looking for .status.ready. Swallowing.", e);
    }
    try {
      return obj.getRaw()
          .get("status")
          .getAsJsonObject()
          .get("state")
          .getAsString()
          .matches("(?i)READY|RUNNING|FINISHED");
    } catch (Exception e) {
      log.debug("Exception looking for .status.state. Swallowing.", e);
    }
    try {
      return obj.getRaw()
          .get("status")
          .getAsJsonObject()
          .get("jobStatus")
          .getAsJsonObject()
          .get("state")
          .getAsString()
          .matches("(?i)READY|RUNNING|FINISHED");
    } catch (Exception e) {
      log.debug("Exception looking for .status.jobStatus.state. Swallowing.", e);
    }
    // TODO: Look for common Conditions
    log.warn("Resource {}/{}/{} considered ready by default.", obj.getMetadata().getNamespace(), obj.getKind(),
        obj.getMetadata().getName());
    return true;
  }

  public boolean isFailed(String yaml) {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    String namespace = obj.getMetadata().getNamespace();
    String name = obj.getMetadata().getName();
    String kind = obj.getKind();
    try {
      KubernetesApiResponse<DynamicKubernetesObject> existing = apiFor(obj).get(namespace, name);
      existing.onFailure((code, status) -> log.warn("Failed to fetch {}/{}: {}.", kind, name, status.getMessage()));
      if (!existing.isSuccess()) {
        return false;
      }
      if (isFailed(existing.getObject())) {
        log.info("{}/{} is FAILED.", kind, name);
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
  }

  public static boolean isFailed(DynamicKubernetesObject obj) {
    // We make a best effort to guess the status of the dynamic object. By default, it's not failed.
    if (obj == null || obj.getRaw() == null) {
      return false;
    }
    try {
      return obj.getRaw().get("status").getAsJsonObject().get("failed").getAsBoolean();
    } catch (Exception e) {
      log.debug("Exception looking for .status.ready. Swallowing.", e);
    }
    try {
      return obj.getRaw().get("status").getAsJsonObject().get("state").getAsString().matches("(?i)FAILED|ERROR");
    } catch (Exception e) {
      log.debug("Exception looking for .status.state. Swallowing.", e);
    }
    try {
      return obj.getRaw()
          .get("status")
          .getAsJsonObject()
          .get("jobStatus")
          .getAsJsonObject()
          .get("state")
          .getAsString()
          .matches("(?i)FAILED|ERROR");
    } catch (Exception e) {
      log.debug("Exception looking for .status.jobStatus.state. Swallowing.", e);
    }
    // TODO: Look for common Conditions
    return false;
  }

  public static class ApiInfo<T extends KubernetesObject, L extends KubernetesListObject> {
    private final String kind;
    private final String singular;
    private final String plural;
    private final String group;
    private final String version;
    private final Class<T> type;
    private final Class<L> list;

    public ApiInfo(String kind, String singular, String plural, String group, String version, Class<T> type,
        Class<L> list) {
      this.kind = kind;
      this.singular = singular;
      this.plural = plural;
      this.group = group;
      this.version = version;
      this.type = type;
      this.list = list;
    }

    public GenericKubernetesApi<T, L> generic(ApiClient apiClient) {
      return new GenericKubernetesApi<>(type(), list(), group(), version(), plural(), apiClient);
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
