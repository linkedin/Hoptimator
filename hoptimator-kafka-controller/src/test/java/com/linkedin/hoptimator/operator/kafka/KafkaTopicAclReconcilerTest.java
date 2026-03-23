package com.linkedin.hoptimator.operator.kafka;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.models.V1alpha1Acl;
import com.linkedin.hoptimator.models.V1alpha1AclList;
import com.linkedin.hoptimator.models.V1alpha1AclSpec;
import com.linkedin.hoptimator.models.V1alpha1AclSpecResource;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicSpec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class KafkaTopicAclReconcilerTest {

  @Mock
  private K8sContext context;

  @Mock
  private K8sApi<V1alpha1KafkaTopic, V1alpha1KafkaTopicList> kafkaTopicApi;

  @Mock
  private K8sApi<V1alpha1Acl, V1alpha1AclList> aclApi;

  @Mock
  private AdminClient mockAdmin;

  @Mock
  private MockedStatic<AdminClient> adminClientStatic;

  private KafkaTopicAclReconciler reconciler;

  @BeforeEach
  void setUp() {
    reconciler = new KafkaTopicAclReconciler(context, kafkaTopicApi, aclApi);
    adminClientStatic.when(() -> AdminClient.create(any(Properties.class))).thenReturn(mockAdmin);
  }

  @Test
  void testReconcileDeletedAclReturnsNoRequeue() throws Exception {
    SQLException notFound = new SQLException("Not found", "42000", 404);
    when(aclApi.get("ns", "deleted-acl")).thenThrow(notFound);

    Result result = reconciler.reconcile(new Request("ns", "deleted-acl"));

    assertFalse(result.isRequeue());
  }

  @Test
  void testReconcileNonKafkaTopicAclSkips() throws Exception {
    V1alpha1Acl acl = buildAcl("SomeOtherKind", "target", "user:alice", V1alpha1AclSpec.MethodEnum.READ);
    when(aclApi.get("ns", "other-acl")).thenReturn(acl);

    Result result = reconciler.reconcile(new Request("ns", "other-acl"));

    assertFalse(result.isRequeue());
  }

  @Test
  void testReconcileUnsupportedMethodSkips() throws Exception {
    V1alpha1Acl acl = buildAcl("KafkaTopic", "target", "user:alice", V1alpha1AclSpec.MethodEnum.DELETE);
    when(aclApi.get("ns", "unsupported-acl")).thenReturn(acl);

    Result result = reconciler.reconcile(new Request("ns", "unsupported-acl"));

    assertFalse(result.isRequeue());
  }

  @Test
  void testReconcileTargetNotFoundRequeues() throws Exception {
    V1alpha1Acl acl = buildAcl("KafkaTopic", "missing-topic", "user:alice", V1alpha1AclSpec.MethodEnum.READ);
    when(aclApi.get("ns", "acl-missing-target")).thenReturn(acl);
    when(kafkaTopicApi.get("ns", "missing-topic")).thenReturn(null);

    Result result = reconciler.reconcile(new Request("ns", "acl-missing-target"));

    assertTrue(result.isRequeue());
  }

  @Test
  void testReconcileReadAclCreatesBinding() throws Exception {
    V1alpha1Acl acl = buildAcl("KafkaTopic", "my-topic-obj", "user:alice", V1alpha1AclSpec.MethodEnum.READ);
    when(aclApi.get("ns", "read-acl")).thenReturn(acl);

    V1alpha1KafkaTopic topic = buildKafkaTopic("actual-topic-name");
    when(kafkaTopicApi.get("ns", "my-topic-obj")).thenReturn(topic);

    CreateAclsResult createResult = mock(CreateAclsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createAcls(any())).thenReturn(createResult);

    Result result = reconciler.reconcile(new Request("ns", "read-acl"));

    assertFalse(result.isRequeue());
    verify(mockAdmin).createAcls(any());
  }

  @Test
  void testReconcileWriteAclCreatesBinding() throws Exception {
    V1alpha1Acl acl = buildAcl("KafkaTopic", "my-topic-obj", "user:bob", V1alpha1AclSpec.MethodEnum.WRITE);
    when(aclApi.get("ns", "write-acl")).thenReturn(acl);

    V1alpha1KafkaTopic topic = buildKafkaTopic("actual-topic-name");
    when(kafkaTopicApi.get("ns", "my-topic-obj")).thenReturn(topic);

    CreateAclsResult createResult = mock(CreateAclsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createAcls(any())).thenReturn(createResult);

    Result result = reconciler.reconcile(new Request("ns", "write-acl"));

    assertFalse(result.isRequeue());
    verify(mockAdmin).createAcls(any());
  }

  @Test
  void testReconcileExceptionRequeues() throws Exception {
    SQLException serverError = new SQLException("Server error", "42000", 500);
    when(aclApi.get("ns", "error-acl")).thenThrow(serverError);

    Result result = reconciler.reconcile(new Request("ns", "error-acl"));

    assertTrue(result.isRequeue());
  }

  @Test
  void testFailureRetryDuration() {
    Duration duration = reconciler.failureRetryDuration();
    assertEquals(Duration.ofMinutes(5), duration);
  }

  @Test
  void testPendingRetryDuration() {
    Duration duration = reconciler.pendingRetryDuration();
    assertEquals(Duration.ofMinutes(1), duration);
  }

  private V1alpha1Acl buildAcl(String targetKind, String targetName, String principal, V1alpha1AclSpec.MethodEnum method) {
    V1alpha1AclSpecResource resource = new V1alpha1AclSpecResource();
    resource.kind(targetKind);
    resource.name(targetName);

    V1alpha1AclSpec spec = new V1alpha1AclSpec();
    spec.setResource(resource);
    spec.setPrincipal(principal);
    spec.setMethod(method);

    V1alpha1Acl acl = new V1alpha1Acl();
    acl.setSpec(spec);
    return acl;
  }

  private V1alpha1KafkaTopic buildKafkaTopic(String topicName) {
    V1alpha1KafkaTopicSpec spec = new V1alpha1KafkaTopicSpec();
    spec.setTopicName(topicName);

    V1alpha1KafkaTopic topic = new V1alpha1KafkaTopic();
    topic.setSpec(spec);
    return topic;
  }
}
