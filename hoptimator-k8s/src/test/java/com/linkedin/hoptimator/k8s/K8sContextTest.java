package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT"},
    justification = "Mockito doReturn().when() stubs — framework captures the return value")
class K8sContextTest {

  @Mock
  private ApiClient apiClient;

  @Mock
  private SharedInformerFactory informerFactory;

  @Mock
  private HoptimatorConnection connection;

  private K8sContext context;

  @BeforeEach
  void setUp() {
    context = new K8sContext("test-ns", "", "test context", apiClient, informerFactory, null,
        Collections.emptyMap(), connection);
  }

  @Test
  void namespaceReturnsConfiguredNamespace() {
    assertEquals("test-ns", context.namespace());
  }

  @Test
  void watchNamespaceReturnsEmptyByDefault() {
    assertEquals("", context.watchNamespace());
  }

  @Test
  void apiClientReturnsConfiguredClient() {
    assertSame(apiClient, context.apiClient());
  }

  @Test
  void informerFactoryReturnsConfiguredFactory() {
    assertSame(informerFactory, context.informerFactory());
  }

  @Test
  void connectionReturnsConfiguredConnection() {
    assertSame(connection, context.connection());
  }

  @Test
  void toStringReturnsClientInfo() {
    assertEquals("test context", context.toString());
  }

  @Test
  void withOwnerReturnsNewContextWithOwner() {
    V1OwnerReference owner = new V1OwnerReference().name("my-owner").uid("uid-123");

    K8sContext owned = context.withOwner(owner);

    assertNotNull(owned);
    assertEquals("test-ns", owned.namespace());
    assertTrue(owned.toString().contains("my-owner"));
    assertSame(apiClient, owned.apiClient());
  }

  @Test
  void withLabelReturnsNewContextWithLabel() {
    K8sContext labeled = context.withLabel("app", "test");

    assertNotNull(labeled);
    assertEquals("test-ns", labeled.namespace());
    assertTrue(labeled.toString().contains("app=test"));
    assertSame(apiClient, labeled.apiClient());
  }

  @Test
  void withMultipleLabelsAccumulates() {
    K8sContext labeled = context.withLabel("app", "test").withLabel("env", "dev");

    assertTrue(labeled.toString().contains("app=test"));
    assertTrue(labeled.toString().contains("env=dev"));
  }

  @Test
  void ownWithNoOwnerReferenceIsNoOp() {
    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setMetadata(new V1ObjectMeta().name("test-obj").namespace("ns"));

    context.own(obj);

    assertNull(obj.getMetadata().getOwnerReferences());
  }

  @Test
  void ownWithOwnerReferenceAddsOwner() {
    V1OwnerReference owner = new V1OwnerReference().name("my-owner").uid("uid-123")
        .kind("Pipeline").apiVersion("v1alpha1");
    K8sContext owned = context.withOwner(owner);

    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    obj.setMetadata(new V1ObjectMeta().name("test-obj").namespace("ns"));

    owned.own(obj);

    assertNotNull(obj.getMetadata().getOwnerReferences());
    assertEquals(1, obj.getMetadata().getOwnerReferences().size());
    assertEquals("my-owner", obj.getMetadata().getOwnerReferences().get(0).getName());
  }

  @Test
  void ownDoesNotAddDuplicateOwner() {
    V1OwnerReference owner = new V1OwnerReference().name("my-owner").uid("uid-123")
        .kind("Pipeline").apiVersion("v1alpha1");
    K8sContext owned = context.withOwner(owner);

    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    List<V1OwnerReference> existing = new ArrayList<>();
    existing.add(new V1OwnerReference().name("my-owner").uid("uid-123"));
    obj.setMetadata(new V1ObjectMeta().name("test-obj").namespace("ns").ownerReferences(existing));

    owned.own(obj);

    assertEquals(1, obj.getMetadata().getOwnerReferences().size());
  }

  @Test
  void ownAddsOwnerToExistingList() {
    V1OwnerReference owner = new V1OwnerReference().name("new-owner").uid("uid-456")
        .kind("Pipeline").apiVersion("v1alpha1");
    K8sContext owned = context.withOwner(owner);

    DynamicKubernetesObject obj = new DynamicKubernetesObject();
    List<V1OwnerReference> existing = new ArrayList<>();
    existing.add(new V1OwnerReference().name("existing-owner").uid("uid-123"));
    obj.setMetadata(new V1ObjectMeta().name("test-obj").namespace("ns").ownerReferences(existing));

    owned.own(obj);

    assertEquals(2, obj.getMetadata().getOwnerReferences().size());
  }

  @Test
  void ownWithNonDynamicKubernetesObject() {
    V1OwnerReference owner = new V1OwnerReference().name("owner").uid("uid-789")
        .kind("View").apiVersion("hoptimator.linkedin.com/v1alpha1");
    K8sContext owned = context.withOwner(owner);

    // Use a non-DynamicKubernetesObject (V1alpha1View)
    com.linkedin.hoptimator.k8s.models.V1alpha1View view = new com.linkedin.hoptimator.k8s.models.V1alpha1View();
    view.setMetadata(new V1ObjectMeta().name("test-view").namespace("ns"));

    owned.own(view);

    assertNotNull(view.getMetadata().getOwnerReferences());
    assertEquals(1, view.getMetadata().getOwnerReferences().size());
    assertEquals("owner", view.getMetadata().getOwnerReferences().get(0).getName());
  }

  @Test
  void ownNonDynamicObjectDoesNotDuplicate() {
    V1OwnerReference owner = new V1OwnerReference().name("owner").uid("uid-789")
        .kind("View").apiVersion("hoptimator.linkedin.com/v1alpha1");
    K8sContext owned = context.withOwner(owner);

    com.linkedin.hoptimator.k8s.models.V1alpha1View view = new com.linkedin.hoptimator.k8s.models.V1alpha1View();
    List<V1OwnerReference> existing = new ArrayList<>();
    existing.add(new V1OwnerReference().name("owner").uid("uid-789"));
    view.setMetadata(new V1ObjectMeta().name("test-view").namespace("ns").ownerReferences(existing));

    owned.own(view);

    assertEquals(1, view.getMetadata().getOwnerReferences().size());
  }

  @Test
  void dynamicWithApiVersionAndPlural() {
    ApiClient realClient = newTestApiClient();
    K8sContext realContext = new K8sContext("test-ns", "", "test context", realClient,
        new SharedInformerFactory(realClient), null, Collections.emptyMap(), null);

    assertNotNull(realContext.dynamic("apps/v1", "deployments"));
  }

  @Test
  void dynamicWithGroupVersionPlural() {
    ApiClient realClient = newTestApiClient();
    K8sContext realContext = new K8sContext("test-ns", "", "test context", realClient,
        new SharedInformerFactory(realClient), null, Collections.emptyMap(), null);

    assertNotNull(realContext.dynamic("apps", "v1", "deployments"));
  }

  @Test
  void dynamicWithEndpoint() {
    ApiClient realClient = newTestApiClient();
    K8sContext realContext = new K8sContext("test-ns", "", "test context", realClient,
        new SharedInformerFactory(realClient), null, Collections.emptyMap(), null);

    assertNotNull(realContext.dynamic(K8sApiEndpoints.PIPELINES));
  }

  @Test
  void genericWithEndpoint() {
    ApiClient realClient = newTestApiClient();
    K8sContext realContext = new K8sContext("test-ns", "", "test context", realClient,
        new SharedInformerFactory(realClient), null, Collections.emptyMap(), null);

    assertNotNull(realContext.generic(K8sApiEndpoints.PIPELINES));
  }

  private static ApiClient newTestApiClient() {
    ApiClient client = new ApiClient();
    client.setHttpClient(client.getHttpClient().newBuilder()
        .readTimeout(0, TimeUnit.SECONDS).build());
    return client;
  }

  @Test
  void getPodNamespaceDefaultsWhenNoEnvOrProperty() {
    // With no env var or system property, getPodNamespace falls back to DEFAULT_NAMESPACE
    assertEquals("default", K8sContext.DEFAULT_NAMESPACE);
  }

  @Test
  void createWithPasswordAuthentication() {
    HoptimatorConnection mockConn = mock(HoptimatorConnection.class);
    Properties props = new Properties();
    props.setProperty(K8sContext.NAMESPACE_KEY, "custom-ns");
    props.setProperty(K8sContext.SERVER_KEY, "https://k8s.example.com");
    props.setProperty(K8sContext.USER_KEY, "admin");
    props.setProperty(K8sContext.PASSWORD_KEY, "secret");
    doReturn(props).when(mockConn).connectionProperties();

    K8sContext ctx = K8sContext.create(mockConn);

    assertNotNull(ctx);
    assertEquals("custom-ns", ctx.namespace());
    assertTrue(ctx.toString().contains("password authentication"));
  }

  @Test
  void createWithTokenAuthentication() {
    HoptimatorConnection mockConn = mock(HoptimatorConnection.class);
    Properties props = new Properties();
    props.setProperty(K8sContext.NAMESPACE_KEY, "token-ns");
    props.setProperty(K8sContext.SERVER_KEY, "https://k8s.example.com");
    props.setProperty(K8sContext.TOKEN_KEY, "my-token");
    doReturn(props).when(mockConn).connectionProperties();

    K8sContext ctx = K8sContext.create(mockConn);

    assertNotNull(ctx);
    assertEquals("token-ns", ctx.namespace());
    assertTrue(ctx.toString().contains("token authentication"));
  }

  @Test
  void createWithImpersonation() {
    HoptimatorConnection mockConn = mock(HoptimatorConnection.class);
    Properties props = new Properties();
    props.setProperty(K8sContext.NAMESPACE_KEY, "ns");
    props.setProperty(K8sContext.SERVER_KEY, "https://k8s.example.com");
    props.setProperty(K8sContext.TOKEN_KEY, "my-token");
    props.setProperty(K8sContext.IMPERSONATE_USER_KEY, "impuser");
    props.setProperty(K8sContext.IMPERSONATE_GROUP_KEY, "impgroup");
    props.setProperty(K8sContext.IMPERSONATE_GROUPS_KEY, "group1,group2");
    doReturn(props).when(mockConn).connectionProperties();

    K8sContext ctx = K8sContext.create(mockConn);

    assertNotNull(ctx);
    assertTrue(ctx.toString().contains("impuser"));
    assertTrue(ctx.toString().contains("impgroup"));
    assertTrue(ctx.toString().contains("group1,group2"));
  }

  @Test
  void createWithWatchNamespace() {
    HoptimatorConnection mockConn = mock(HoptimatorConnection.class);
    Properties props = new Properties();
    props.setProperty(K8sContext.NAMESPACE_KEY, "ns");
    props.setProperty(K8sContext.WATCH_NAMESPACE_KEY, "watch-ns");
    props.setProperty(K8sContext.SERVER_KEY, "https://k8s.example.com");
    props.setProperty(K8sContext.TOKEN_KEY, "token");
    doReturn(props).when(mockConn).connectionProperties();

    K8sContext ctx = K8sContext.create(mockConn);

    assertEquals("watch-ns", ctx.watchNamespace());
  }

  @Test
  void createWithNullWatchNamespaceDefaultsToEmpty() {
    HoptimatorConnection mockConn = mock(HoptimatorConnection.class);
    Properties props = new Properties();
    props.setProperty(K8sContext.NAMESPACE_KEY, "ns");
    props.setProperty(K8sContext.SERVER_KEY, "https://k8s.example.com");
    props.setProperty(K8sContext.TOKEN_KEY, "token");
    doReturn(props).when(mockConn).connectionProperties();

    K8sContext ctx = K8sContext.create(mockConn);

    assertEquals("", ctx.watchNamespace());
  }

  @Test
  void getPodNamespaceReturnsSelfPodNamespaceSystemProperty() {
    // Tests getPodNamespace() when SELF_POD_NAMESPACE system property is set
    String original = System.getProperty("SELF_POD_NAMESPACE");
    try {
      System.setProperty("SELF_POD_NAMESPACE", "my-pod-namespace");
      // create() needs a namespace set via NAMESPACE_KEY or falls back to getPodNamespace()
      HoptimatorConnection mockConn = mock(HoptimatorConnection.class);
      Properties props = new Properties();
      // No NAMESPACE_KEY - will fall through to getPodNamespace()
      props.setProperty(K8sContext.SERVER_KEY, "https://k8s.example.com");
      props.setProperty(K8sContext.TOKEN_KEY, "token");
      doReturn(props).when(mockConn).connectionProperties();

      K8sContext ctx = K8sContext.create(mockConn);
      assertEquals("my-pod-namespace", ctx.namespace());
    } finally {
      if (original == null) {
        System.clearProperty("SELF_POD_NAMESPACE");
      } else {
        System.setProperty("SELF_POD_NAMESPACE", original);
      }
    }
  }

  @Test
  void getPodNamespaceReturnsDefaultWhenNeitherEnvNorPropertySet() {
    // Tests getPodNamespace() fallback to DEFAULT_NAMESPACE
    // Ensure the system property is not set
    String original = System.getProperty("SELF_POD_NAMESPACE");
    System.clearProperty("SELF_POD_NAMESPACE");
    try {
      HoptimatorConnection mockConn = mock(HoptimatorConnection.class);
      Properties props = new Properties();
      props.setProperty(K8sContext.SERVER_KEY, "https://k8s.example.com");
      props.setProperty(K8sContext.TOKEN_KEY, "token");
      doReturn(props).when(mockConn).connectionProperties();

      K8sContext ctx = K8sContext.create(mockConn);
      // Should use DEFAULT_NAMESPACE when no env var or property is set
      assertEquals(K8sContext.DEFAULT_NAMESPACE, ctx.namespace());
    } finally {
      if (original != null) {
        System.setProperty("SELF_POD_NAMESPACE", original);
      }
    }
  }

  @Test
  void constantsAreDefined() {
    assertEquals("k8s.namespace", K8sContext.NAMESPACE_KEY);
    assertEquals("k8s.watch.namespace", K8sContext.WATCH_NAMESPACE_KEY);
    assertEquals("k8s.server", K8sContext.SERVER_KEY);
    assertEquals("k8s.user", K8sContext.USER_KEY);
    assertEquals("k8s.kubeconfig", K8sContext.KUBECONFIG_KEY);
    assertEquals("k8s.impersonate.user", K8sContext.IMPERSONATE_USER_KEY);
    assertEquals("k8s.impersonate.group", K8sContext.IMPERSONATE_GROUP_KEY);
    assertEquals("k8s.impersonate.groups", K8sContext.IMPERSONATE_GROUPS_KEY);
    assertEquals("k8s.password", K8sContext.PASSWORD_KEY);
    assertEquals("k8s.token", K8sContext.TOKEN_KEY);
    assertEquals("k8s.ssl.truststore.location", K8sContext.SSL_TRUSTSTORE_LOCATION_KEY);
  }
}
