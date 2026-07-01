package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.operator.trigger.TriggerInput;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaInputWatermarkTest {

  private static final Node NODE = new Node(0, "localhost", 9092);
  private final AdminClient admin = mock(AdminClient.class);
  private final KafkaInputWatermark provider = new KafkaInputWatermark(admin);

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

    assertEquals(Optional.of(Instant.ofEpochMilli(3_000L)),
        provider.completeThrough(new TriggerInput("KAFKA", "cluster", "topic1")));
  }

  @Test
  void emptyWhenAllPartitionsEmpty() {
    topicWithPartitions("topic1", 2);
    maxTimestamps("topic1", -1L, -1L);

    assertFalse(provider.completeThrough(new TriggerInput(null, "cluster", "topic1")).isPresent());
  }

  @Test
  void declinesNonKafkaCatalogWithoutTouchingAdmin() {
    assertTrue(provider.completeThrough(new TriggerInput("OPENHOUSE", "db", "t")).isEmpty());
    verify(admin, never()).describeTopics(anyCollection());
  }

  @Test
  void emptyWhenInputHasNoTable() {
    assertTrue(provider.completeThrough(new TriggerInput(null, "cluster", null)).isEmpty());
  }
}
