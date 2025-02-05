package com.linkedin.hoptimator.k8s;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

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
  public static final String DEFAULT_NAMESPACE = "default";
  public static final String NAMESPACE_KEY = "k8s.namespace";
  public static final String SERVER_KEY = "k8s.server";
  public static final String USER_KEY = "k8s.user";
  public static final String PASSWORD_KEY = "k8s.password";
  public static final String TOKEN_KEY = "k8s.token";
  public static final String SSL_TRUSTSTORE_LOCATION_KEY = "k8s.ssl.truststore.location";

  private final String namespace;
  private final String clientInfo;
  private ApiClient apiClient;
  private final SharedInformerFactory informerFactory;

  public K8sContext(Properties connectionProperties) {
    if (connectionProperties.getProperty(NAMESPACE_KEY) != null) {
      this.namespace = connectionProperties.getProperty(NAMESPACE_KEY);
    } else {
      this.namespace = getPodNamespace();
    }
    String server = connectionProperties.getProperty(SERVER_KEY);
    String user = connectionProperties.getProperty(USER_KEY);
    String password = connectionProperties.getProperty(PASSWORD_KEY);
    String token = connectionProperties.getProperty(TOKEN_KEY);
    String truststore = connectionProperties.getProperty(SSL_TRUSTSTORE_LOCATION_KEY);

    if (server != null && user != null && password != null) {
      this.clientInfo = "User " + user + " accessing " + server + " via password authentication";
      this.apiClient = Config.fromUserPassword(server, user, password);
    } else if (server != null && token != null) {
      this.clientInfo = "Accessing " + server + " via token authentication";
      this.apiClient = Config.fromToken(server, token);
      this.apiClient.setApiKeyPrefix("Bearer");
    } else if (server != null) {
      this.clientInfo = "Using default configuration from ./kube/config to access " + server;
      try {
        this.apiClient = Config.defaultClient();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      this.apiClient.setBasePath(server);
    } else {
      this.clientInfo = "Using default configuration from ./kube/config";
      try {
        this.apiClient = Config.defaultClient();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    if (truststore != null) {
      try {
        InputStream in = Files.newInputStream(Paths.get(truststore));
        apiClient.setSslCaCert(in);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    this.informerFactory = new SharedInformerFactory(apiClient);
  }

  public ApiClient apiClient() {
    return apiClient;
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
    return clientInfo;
  }

  private static String getPodNamespace() {
    String filePath = System.getenv("POD_NAMESPACE_FILEPATH");
    if (filePath != null) {
      try {
        return new String(Files.readAllBytes(Paths.get(filePath)));
      } catch (IOException e) {
        // swallow
      }
    }
    String namespace = System.getProperty("SELF_POD_NAMESPACE");
    if (namespace != null) {
      return namespace;
    }
    return DEFAULT_NAMESPACE;
  }
}
