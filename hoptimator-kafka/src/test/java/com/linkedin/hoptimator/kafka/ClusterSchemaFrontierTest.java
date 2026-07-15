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
 * {@code AdminClient}. Verifies the bounded-out-of-orderness watermark: the per-partition minimum of
 * {@code maxTimestamp} held back by the lag {@code B}, excluding partitions more than {@code B}
 * behind the leader.
 */
class ClusterSchemaFrontierTest {

  private static final long LAG_MS = 1_000L;
  private static final Node NODE = new Node(0, "localhost", 9092);
  private final AdminClient admin = mock(AdminClient.class);

  private ClusterSchema schema() {
    return schema(LAG_MS);
  }

  private ClusterSchema schema(long lagMs) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put(ClusterSchema.FRONTIER_LAG_MS_PROPERTY, Long.toString(lagMs));
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
  void balancedPartitionsUseMinMinusLag() {
    // Both partitions within B of the leader -> min(10_000, 10_500) - B.
    topicWithPartitions("topic1", 2);
    maxTimestamps("topic1", 10_000L, 10_500L);

    assertThat(schema().frontier("topic1")).contains(Instant.ofEpochMilli(10_000L - LAG_MS));
  }

  @Test
  void withinLagLaggardHoldsBackWatermark() {
    // Partition 1 is 800ms behind the leader (< B), so it stays in the min and holds the watermark.
    topicWithPartitions("topic1", 2);
    maxTimestamps("topic1", 10_000L, 9_200L);

    assertThat(schema().frontier("topic1")).contains(Instant.ofEpochMilli(9_200L - LAG_MS));
  }

  @Test
  void excludesStragglerMoreThanLagBehind() {
    // Partition 1 is 5_000ms behind the leader (>> B) -> treated as idle, excluded, so it does NOT
    // drag the watermark down to 5_000 - B.
    topicWithPartitions("topic1", 2);
    maxTimestamps("topic1", 10_000L, 5_000L);

    assertThat(schema().frontier("topic1")).contains(Instant.ofEpochMilli(10_000L - LAG_MS));
  }

  @Test
  void singlePartitionIsMaxMinusLag() {
    topicWithPartitions("topic1", 1);
    maxTimestamps("topic1", 10_000L);

    assertThat(schema().frontier("topic1")).contains(Instant.ofEpochMilli(10_000L - LAG_MS));
  }

  @Test
  void respectsConfiguredLag() {
    topicWithPartitions("topic1", 1);
    maxTimestamps("topic1", 10_000L);

    assertThat(schema(2_000L).frontier("topic1")).contains(Instant.ofEpochMilli(8_000L));
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
