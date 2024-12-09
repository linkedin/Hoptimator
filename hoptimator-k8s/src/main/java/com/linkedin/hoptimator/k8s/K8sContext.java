package com.linkedin.hoptimator.k8s;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;

import io.kubernetes.client.apimachinery.GroupVersion;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;


public class K8sContext {

  private static K8sContext currentContext = null;

  private final String name;
  private final String namespace;
  private final ApiClient apiClient;
  private final SharedInformerFactory informerFactory;

  public K8sContext(String name, String namespace, ApiClient apiClient) {
    this.name = name;
    this.namespace = namespace;
    this.apiClient = apiClient;
    this.informerFactory = new SharedInformerFactory(apiClient);
  }

  public ApiClient apiClient() {
    return apiClient;
  }

  public String name() {
    return name;
  }

  public String namespace() {
    return namespace;
  }

  public SharedInformerFactory informerFactory() {
    return informerFactory;
  }

  public <T extends KubernetesObject, U extends KubernetesListObject> void registerInformer(
      K8sApiEndpoint<T, U> endpoint, Duration resyncPeriod) {
    informerFactory.sharedIndexInformerFor(generic(endpoint), endpoint.elementType(), resyncPeriod.toMillis());
  }

  public DynamicKubernetesApi dynamic(String apiVersion, String plural) {
    GroupVersion gv = GroupVersion.parse(apiVersion);
    return dynamic(gv.getGroup(), gv.getVersion(), plural);
  }

  public DynamicKubernetesApi dynamic(String group, String version, String plural) {
    return new DynamicKubernetesApi(group, version, plural, apiClient);
  }

  public DynamicKubernetesApi dynamic(K8sApiEndpoint<?, ?> endpoint) {
    return dynamic(endpoint.group(), endpoint.version(), endpoint.plural());
  }

  public <T extends KubernetesObject, U extends KubernetesListObject> GenericKubernetesApi<T, U> generic(Class<T> t,
      Class<U> u, String group, String version, String plural) {
    return new GenericKubernetesApi<T, U>(t, u, group, version, plural, apiClient);
  }

  public <T extends KubernetesObject, U extends KubernetesListObject> GenericKubernetesApi<T, U> generic(
      K8sApiEndpoint<T, U> endpoint) {
    return generic(endpoint.elementType(), endpoint.listType(), endpoint.group(), endpoint.version(),
        endpoint.plural());
  }

  @Override
  public String toString() {
    return name + "/" + namespace;
  }

  public static K8sContext currentContext() {
    if (currentContext == null) {
      try {
        currentContext = defaultContext();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return currentContext;
  }

  public static void useContext(K8sContext context) {
    currentContext = context;
  }

  static K8sContext defaultContext() throws IOException {
    File file = Paths.get(System.getProperty("user.home"), ".kube", "config").toFile();
    try (Reader r = Files.newBufferedReader(file.toPath())) {
      KubeConfig kubeConfig = KubeConfig.loadKubeConfig(r);
      kubeConfig.setFile(file);
      ApiClient apiClient = ClientBuilder.kubeconfig(kubeConfig).build();
      String namespace = Optional.ofNullable(kubeConfig.getNamespace()).orElse("default");
      return new K8sContext(kubeConfig.getCurrentContext(), namespace, apiClient);
    }
  }
}
