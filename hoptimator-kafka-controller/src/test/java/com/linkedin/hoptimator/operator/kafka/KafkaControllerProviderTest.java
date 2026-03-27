package com.linkedin.hoptimator.operator.kafka;

import java.time.Duration;
import java.util.Collection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.generic.GenericKubernetesApi;

import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.models.V1alpha1Acl;
import com.linkedin.hoptimator.models.V1alpha1AclList;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class KafkaControllerProviderTest {

  @Mock
  private K8sContext context;

  @Test
  void testKafkaTopicsEndpointKind() {
    assertEquals("KafkaTopic", KafkaControllerProvider.KAFKA_TOPICS.kind());
  }

  @Test
  void testKafkaTopicsEndpointGroup() {
    assertEquals("hoptimator.linkedin.com", KafkaControllerProvider.KAFKA_TOPICS.group());
  }

  @Test
  void testKafkaTopicsEndpointVersion() {
    assertEquals("v1alpha1", KafkaControllerProvider.KAFKA_TOPICS.version());
  }

  @Test
  void testKafkaTopicsEndpointPlural() {
    assertEquals("kafkatopics", KafkaControllerProvider.KAFKA_TOPICS.plural());
  }

  @Test
  void testKafkaTopicsEndpointNotClusterScoped() {
    assertFalse(KafkaControllerProvider.KAFKA_TOPICS.clusterScoped());
  }

  @Test
  void testAclsEndpointKind() {
    assertEquals("Acl", KafkaControllerProvider.ACLS.kind());
  }

  @Test
  void testAclsEndpointGroup() {
    assertEquals("hoptimator.linkedin.com", KafkaControllerProvider.ACLS.group());
  }

  @Test
  void testAclsEndpointNotClusterScoped() {
    assertFalse(KafkaControllerProvider.ACLS.clusterScoped());
  }

  @Test
  void testProviderIsInstantiable() {
    KafkaControllerProvider provider = new KafkaControllerProvider();
    assertNotNull(provider);
  }

  @Test
  void controllersReturnsTwoControllers() {
    ApiClient apiClient = mock(ApiClient.class);
    SharedInformerFactory informerFactory = new SharedInformerFactory(apiClient);

    // Pre-register the informers that ControllerBuilder.watch() will look for
    GenericKubernetesApi<V1alpha1KafkaTopic, V1alpha1KafkaTopicList> topicApi =
        new GenericKubernetesApi<>(V1alpha1KafkaTopic.class, V1alpha1KafkaTopicList.class,
            "hoptimator.linkedin.com", "v1alpha1", "kafkatopics", apiClient);
    informerFactory.sharedIndexInformerFor(topicApi, V1alpha1KafkaTopic.class, 300000L, "");

    GenericKubernetesApi<V1alpha1Acl, V1alpha1AclList> aclApi =
        new GenericKubernetesApi<>(V1alpha1Acl.class, V1alpha1AclList.class,
            "hoptimator.linkedin.com", "v1alpha1", "acls", apiClient);
    informerFactory.sharedIndexInformerFor(aclApi, V1alpha1Acl.class, 300000L, "");

    when(context.informerFactory()).thenReturn(informerFactory);
    doAnswer(inv -> null).when(context).registerInformer(any(), any(Duration.class));

    KafkaControllerProvider provider = new KafkaControllerProvider();
    Collection<Controller> controllers = provider.controllers(context);

    assertNotNull(controllers);
    assertEquals(2, controllers.size());
  }
}
