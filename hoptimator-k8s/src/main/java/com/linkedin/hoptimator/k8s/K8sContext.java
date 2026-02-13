package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import io.kubernetes.client.apimachinery.GroupVersion;
import io.kubernetes.client.common.KubernetesListObject;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;


public final class K8sContext {
  public static final String DEFAULT_NAMESPACE = "default";
  public static final String NAMESPACE_KEY = "k8s.namespace";
  public static final String WATCH_NAMESPACE_KEY = "k8s.watch.namespace";
  public static final String SERVER_KEY = "k8s.server";
  public static final String USER_KEY = "k8s.user";
  public static final String KUBECONFIG_KEY = "k8s.kubeconfig";
  public static final String IMPERSONATE_USER_KEY = "k8s.impersonate.user";
  public static final String IMPERSONATE_GROUP_KEY = "k8s.impersonate.group";
  public static final String IMPERSONATE_GROUPS_KEY = "k8s.impersonate.groups";
  public static final String PASSWORD_KEY = "k8s.password";
  public static final String TOKEN_KEY = "k8s.token";
  public static final String SSL_TRUSTSTORE_LOCATION_KEY = "k8s.ssl.truststore.location";

  private final String namespace;
  private final String watchNamespace;
  private final String clientInfo;
  private final ApiClient apiClient;
  private final SharedInformerFactory informerFactory;
  private final V1OwnerReference ownerReference;
  private final Map<String, String> labels;
  private final HoptimatorConnection connection;

  private K8sContext(String namespace, String watchNamespace, String clientInfo, ApiClient apiClient,
      SharedInformerFactory informerFactory, V1OwnerReference ownerReference, Map<String, String> labels,
      HoptimatorConnection connection) {
    this.namespace = namespace;
    this.watchNamespace = watchNamespace;
    this.clientInfo = clientInfo;
    this.apiClient = apiClient;
    this.informerFactory = informerFactory;
    this.ownerReference = ownerReference;
    this.labels = labels;
    this.connection = connection;
  }

  public static K8sContext create(Connection connection) {
    String namespace;
    ApiClient apiClient;
    String info;

    HoptimatorConnection hoptimatorConnection = (HoptimatorConnection) connection;
    Properties connectionProperties = hoptimatorConnection.connectionProperties();
    if (connectionProperties.getProperty(NAMESPACE_KEY) != null) {
      namespace = connectionProperties.getProperty(NAMESPACE_KEY);
    } else {
      namespace = getPodNamespace();
    }
    String watchNamespace = connectionProperties.getProperty(WATCH_NAMESPACE_KEY);
    String kubeconfig = connectionProperties.getProperty(KUBECONFIG_KEY);
    String server = connectionProperties.getProperty(SERVER_KEY);
    String user = connectionProperties.getProperty(USER_KEY);
    String impersonateUser = connectionProperties.getProperty(IMPERSONATE_USER_KEY);
    String impersonateGroup = connectionProperties.getProperty(IMPERSONATE_GROUP_KEY);
    String impersonateGroups = connectionProperties.getProperty(IMPERSONATE_GROUPS_KEY);
    String password = connectionProperties.getProperty(PASSWORD_KEY);
    String token = connectionProperties.getProperty(TOKEN_KEY);
    String truststore = connectionProperties.getProperty(SSL_TRUSTSTORE_LOCATION_KEY);

    if (server != null && user != null && password != null) {
      info = "User " + user + " using password authentication.";
      apiClient = Config.fromUserPassword(server, user, password);
    } else if (server != null && token != null) {
      info = "Using token authentication.";
      apiClient = Config.fromToken(server, token);
      apiClient.setApiKeyPrefix("Bearer");
    } else {
      if (kubeconfig == null) {
        kubeconfig = System.getProperty("user.home") + "/.kube/config";
      }
      info = "Using kubeconfig from " + kubeconfig + ".";
      try (Reader r = Files.newBufferedReader(Paths.get(kubeconfig))) {
        KubeConfig kubeConfig = KubeConfig.loadKubeConfig(r);
        kubeConfig.setFile(new File(kubeconfig));
        if (namespace == null) {
          namespace = kubeConfig.getNamespace();
        }
        if (namespace == null) {
          namespace = DEFAULT_NAMESPACE;
        }
        apiClient = ClientBuilder.kubeconfig(kubeConfig).build();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    if (server != null) {
      info += " Accessing " + server + ".";
      apiClient.setBasePath(server);
    }

    info += " Using namespace " + namespace + ".";

    if (truststore != null) {
      try {
        InputStream in = Files.newInputStream(Paths.get(truststore));
        apiClient.setSslCaCert(in);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    if (impersonateUser != null) {
      info = "User is " + impersonateUser + ". " + info;
      apiClient.addDefaultHeader("Impersonate-User", impersonateUser);
    }

    if (impersonateGroup != null) {
      info = "Group is " + impersonateGroup + ". " + info;
      apiClient.addDefaultHeader("Impersonate-Group", impersonateGroup);
    }

    // Impersonate-Group header can be applied repeatedly
    if (impersonateGroups != null) {
      info = info + " Impersonating groups " + impersonateGroups + ".";
      for (String x : impersonateGroups.split(",")) {
        apiClient.addDefaultHeader("Impersonate-Group", x);
      }
    }

    if (watchNamespace == null) {
      watchNamespace = "";
    }

    return new K8sContext(namespace, watchNamespace, info, apiClient, new SharedInformerFactory(apiClient),
        null, Collections.emptyMap(), hoptimatorConnection);
  }

  public K8sContext withOwner(V1OwnerReference owner) {
    return new K8sContext(namespace, watchNamespace, clientInfo + " Owner is " + owner.getName() + ".", apiClient,
        informerFactory, owner, labels, connection);
  }

  public K8sContext withLabel(String key, String value) {
    Map<String, String> newLabels = new HashMap<>(labels);
    newLabels.put(key, value);
    return new K8sContext(namespace, watchNamespace, clientInfo + " Label " + key + "=" + value + ".", apiClient,
        informerFactory, ownerReference, newLabels, connection);
  }

  public ApiClient apiClient() {
    return apiClient;
  }

  public String namespace() {
    return namespace;
  }

  public String watchNamespace() {
    return watchNamespace;
  }

  public SharedInformerFactory informerFactory() {
    return informerFactory;
  }

  public <T extends KubernetesObject, U extends KubernetesListObject> void registerInformer(
      K8sApiEndpoint<T, U> endpoint, Duration resyncPeriod) {
    registerInformer(endpoint, resyncPeriod, watchNamespace);
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
    return new GenericKubernetesApi<>(t, u, group, version, plural, apiClient);
  }

  public <T extends KubernetesObject, U extends KubernetesListObject> GenericKubernetesApi<T, U> generic(
      K8sApiEndpoint<T, U> endpoint) {
    return generic(endpoint.elementType(), endpoint.listType(), endpoint.group(), endpoint.version(),
        endpoint.plural());
  }

  /** Take ownership of the object, if this context has an owner. */
  public void own(KubernetesObject obj) {
    if (ownerReference == null) {
      return;
    }
    List<V1OwnerReference> owners = obj.getMetadata().getOwnerReferences();
    if (owners == null) {
      owners = new ArrayList<>();
    }
    if (owners.stream().noneMatch(x -> x.getUid().equals(ownerReference.getUid()))) {
      if (obj instanceof DynamicKubernetesObject) {
        ((DynamicKubernetesObject) obj).setMetadata(obj.getMetadata()
            .namespace(namespace)
            .addOwnerReferencesItem(ownerReference));
      } else {
        owners.add(ownerReference);
        obj.getMetadata().namespace(namespace).ownerReferences(owners);
      }
    }
  }

  public HoptimatorConnection connection() {
    return connection;
  }

  @Override
  public String toString() {
    return clientInfo;
  }

  private static String getPodNamespace() {
    String filePath = System.getenv("POD_NAMESPACE_FILEPATH");
    if (filePath != null) {
      try {
        return Files.readString(Paths.get(filePath));
      } catch (IOException e) {
        // swallow
      }
    }
    String namespace = System.getProperty("SELF_POD_NAMESPACE");
    if (namespace != null) {
      return namespace;
    }
    return null;
  }
}
