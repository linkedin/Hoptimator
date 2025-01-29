package com.linkedin.hoptimator.k8s;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.apimachinery.GroupVersion;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;


public class K8sContext {

  private final static Logger LOG = LoggerFactory.getLogger(K8sContext.class);
  public static final String DEFAULT_NAMESPACE = "default";
  private static final String ENV_OVERRIDE_BASEPATH = "KUBECONFIG_BASEPATH";
  private static K8sContext currentContext = null;

  private final String name;
  private final String namespace;
  private ApiClient apiClient;
  private final SharedInformerFactory informerFactory;

  public K8sContext(String name, String namespace, ApiClient apiClient) {
    LOG.info("K8sContext created for namespace: {}", namespace);
    this.name = name;
    this.namespace = namespace;
    this.apiClient = apiClient;
    this.informerFactory = new SharedInformerFactory(apiClient);
  }

  public ApiClient apiClient() {
    return apiClient;
  }

  // Assigning a new api client should only happen once right after context creation.
  // Re-assigning a new api client can have unexpected consequences.
  public void apiClient(ApiClient apiClient) {
    this.apiClient = apiClient;
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
      K8sApiEndpoint<T, U> endpoint, Duration resyncPeriod, String watchNamespace) {
    informerFactory.sharedIndexInformerFor(generic(endpoint), endpoint.elementType(), resyncPeriod.toMillis(), watchNamespace);
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


  // If $HOME/.kube/config is defined, use that config file.
  // If POD_NAMESPACE_FILEPATH is defined, use that config file.
  // Use Config.defaultClient() and defaultNamespace if no config file is found.
  static K8sContext defaultContext() throws IOException {
    Path path = Paths.get(System.getProperty("user.home"), ".kube", "config");
    if (Files.exists(path)) {
      File file = path.toFile();
      try (Reader r = Files.newBufferedReader(file.toPath())) {
        KubeConfig kubeConfig = KubeConfig.loadKubeConfig(r);
        kubeConfig.setFile(file);
        ApiClient apiClient = addEnvOverrides(kubeConfig).build();
        String namespace = Optional.ofNullable(kubeConfig.getNamespace()).orElse(DEFAULT_NAMESPACE);
        return new K8sContext(kubeConfig.getCurrentContext(), namespace, apiClient);
      }
    } else {
      ApiClient apiClient = Config.defaultClient();
      String namespace = getNamespace();
      return new K8sContext(namespace, namespace, apiClient);
    }
  }

  private static ClientBuilder addEnvOverrides(KubeConfig kubeConfig) throws IOException {
    ClientBuilder builder = ClientBuilder.kubeconfig(kubeConfig);

    String basePath = System.getenv(ENV_OVERRIDE_BASEPATH);
    if (StringUtils.isNotBlank(basePath)) {
      builder.setBasePath(basePath);
    }

    return builder;
  }

  private static String getNamespace() throws IOException {
    String filePath = System.getenv("POD_NAMESPACE_FILEPATH");
    if (filePath != null) {
      return new String(Files.readAllBytes(Paths.get(filePath)));
    }
    String namespace = System.getProperty("SELF_POD_NAMESPACE");
    if (namespace != null) {
      return namespace;
    }
    return DEFAULT_NAMESPACE;
  }
}
