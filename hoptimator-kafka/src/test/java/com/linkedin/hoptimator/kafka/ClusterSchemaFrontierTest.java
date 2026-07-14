package com.linkedin.hoptimator.kafka;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link ClusterSchema}'s {@code InputFrontierSource} capability with a mocked
 * {@code AdminClient}. Verifies the event-time frontier computation (max record timestamp across
 * partitions) and the empty-frontier branches, independent of a live cluster.
 */
class ClusterSchemaFrontierTest {

  private static final Node NODE = new Node(0, "localhost", 9092);
  private final AdminClient admin = mock(AdminClient.class);

  private ClusterSchema schema() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    return new ClusterSchema(properties) {
      @Override
      AdminClient adminClient() {
        return admin;
      }
    };
  }

  private void topicWithPartitions(String topic, int count) {
    TopicPartitionInfo[] partitions = new TopicPartitionInfo[count];
    for (int i = 0; i < count; i++) {
      partitions[i] = new TopicPartitionInfo(i, NODE, List.of(NODE), List.of(NODE));
    }
    TopicDescription description = new TopicDescription(topic, false, Arrays.asList(partitions));
    DescribeTopicsResult result = mock(DescribeTopicsResult.class);
    when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(topic, description)));
    when(admin.describeTopics(anyCollection())).thenReturn(result);
  }

  private void maxTimestamps(String topic, long... timestampsByPartition) {
    Map<TopicPartition, ListOffsetsResultInfo> offsets = new HashMap<>();
    for (int i = 0; i < timestampsByPartition.length; i++) {
      offsets.put(new TopicPartition(topic, i),
          new ListOffsetsResultInfo(i, timestampsByPartition[i], Optional.empty()));
    }
    ListOffsetsResult result = mock(ListOffsetsResult.class);
    when(result.all()).thenReturn(KafkaFuture.completedFuture(offsets));
    when(admin.listOffsets(any())).thenReturn(result);
  }

  @Test
  void returnsLatestRecordTimestampAcrossPartitions() {
    topicWithPartitions("topic1", 2);
    maxTimestamps("topic1", 1_000L, 3_000L);

    assertThat(schema().frontier("topic1")).contains(Instant.ofEpochMilli(3_000L));
  }

  @Test
  void emptyWhenAllPartitionsEmpty() {
    topicWithPartitions("topic1", 2);
    maxTimestamps("topic1", -1L, -1L);

    assertThat(schema().frontier("topic1")).isEmpty();
  }

  @Test
  void emptyWhenTopicNull() {
    assertThat(schema().frontier(null)).isEmpty();
  }

  @Test
  void emptyWhenAdminThrows() {
    when(admin.describeTopics(anyCollection())).thenThrow(new RuntimeException("boom"));

    assertThat(schema().frontier("topic1")).isEmpty();
  }

  @Test
  void changesSinceIsEmptyByDefault() {
    assertThat(schema().changesSince("topic1", Instant.now())).isEmpty();
  }
}
