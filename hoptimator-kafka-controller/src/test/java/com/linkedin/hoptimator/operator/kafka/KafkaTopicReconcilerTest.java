package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicSpec;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicStatus;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class KafkaTopicReconcilerTest {

  @Mock
  private K8sContext context;

  @Mock
  private K8sApi<V1alpha1KafkaTopic, V1alpha1KafkaTopicList> kafkaTopicApi;

  @Mock
  private AdminClient mockAdmin;

  @Mock
  private MockedStatic<AdminClient> adminClientStatic;

  private KafkaTopicReconciler reconciler;

  @BeforeEach
  void setUp() {
    reconciler = new KafkaTopicReconciler(context, kafkaTopicApi);
    adminClientStatic.when(() -> AdminClient.create(any(Properties.class))).thenReturn(mockAdmin);
  }

  @Test
  void testReconcileDeletedObjectReturnsNoRequeue() throws Exception {
    SQLException notFound = new SQLException("Not found", "42000", 404);
    when(kafkaTopicApi.get("ns", "deleted-topic")).thenThrow(notFound);

    Result result = reconciler.reconcile(new Request("ns", "deleted-topic"));

    assertFalse(result.isRequeue());
  }

  @Test
  void testReconcileNonDeletedSqlExceptionRequeues() throws Exception {
    SQLException serverError = new SQLException("Server error", "42000", 500);
    when(kafkaTopicApi.get("ns", "error-topic")).thenThrow(serverError);

    Result result = reconciler.reconcile(new Request("ns", "error-topic"));

    assertTrue(result.isRequeue());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testReconcileCreatesNewTopic() throws Exception {
    V1alpha1KafkaTopic topic = buildKafkaTopic("my-topic", 5, 3);
    when(kafkaTopicApi.get("ns", "my-topic-obj")).thenReturn(topic);

    // Admin describe throws UnknownTopicOrPartitionException (topic doesn't exist yet)
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<Map<String, TopicDescription>> allFuture = mock(KafkaFuture.class);
    when(allFuture.get()).thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("not found")));
    when(describeResult.allTopicNames()).thenReturn(allFuture);
    when(mockAdmin.describeTopics(anyCollection())).thenReturn(describeResult);

    // CreateTopics succeeds
    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createTopics(any())).thenReturn(createResult);

    Result result = reconciler.reconcile(new Request("ns", "my-topic-obj"));

    assertFalse(result.isRequeue());
    verify(mockAdmin).createTopics(any());
    verify(kafkaTopicApi).updateStatus(eq(topic), any(V1alpha1KafkaTopicStatus.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  void testReconcileExistingTopicIncreasesPartitions() throws Exception {
    V1alpha1KafkaTopic topic = buildKafkaTopic("existing-topic", 10, null);
    when(kafkaTopicApi.get("ns", "existing-topic-obj")).thenReturn(topic);

    // Topic exists with 5 partitions
    TopicDescription topicDesc = mock(TopicDescription.class);
    List<TopicPartitionInfo> partitions = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      partitions.add(mock(TopicPartitionInfo.class));
    }
    when(topicDesc.partitions()).thenReturn(partitions);

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<Map<String, TopicDescription>> allFuture = KafkaFuture.completedFuture(
        Map.of("existing-topic", topicDesc));
    when(describeResult.allTopicNames()).thenReturn(allFuture);
    when(mockAdmin.describeTopics(anyCollection())).thenReturn(describeResult);

    CreatePartitionsResult partitionsResult = mock(CreatePartitionsResult.class);
    KafkaFuture<Void> partFuture = KafkaFuture.completedFuture(null);
    when(partitionsResult.all()).thenReturn(partFuture);
    when(mockAdmin.createPartitions(any())).thenReturn(partitionsResult);

    Result result = reconciler.reconcile(new Request("ns", "existing-topic-obj"));

    assertFalse(result.isRequeue());
    verify(mockAdmin).createPartitions(any());
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

  private V1alpha1KafkaTopic buildKafkaTopic(String topicName, Integer numPartitions, Integer replicationFactor) {
    V1alpha1KafkaTopicSpec spec = new V1alpha1KafkaTopicSpec();
    spec.setTopicName(topicName);
    spec.setNumPartitions(numPartitions);
    spec.setReplicationFactor(replicationFactor);

    V1alpha1KafkaTopic topic = new V1alpha1KafkaTopic();
    topic.setSpec(spec);
    return topic;
  }
}
