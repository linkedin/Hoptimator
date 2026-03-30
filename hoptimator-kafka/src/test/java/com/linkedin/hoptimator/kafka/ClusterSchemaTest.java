package com.linkedin.hoptimator.kafka;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ClusterSchemaTest {

  @Mock
  private MockedStatic<AdminClient> adminClientStatic;

  @Mock
  private AdminClient mockAdminClient;

  @Mock
  private ListTopicsResult mockListTopicsResult;

  @Mock
  private DescribeTopicsResult mockDescribeTopicsResult;

  @Mock
  private KafkaFuture<Set<String>> mockNamesFuture;

  @Mock
  private KafkaFuture<TopicDescription> mockTopicDescFuture;

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");

    adminClientStatic.when(() -> AdminClient.create(any(Properties.class)))
        .thenReturn(mockAdminClient);
  }

  @Test
  void tablesGetReturnsTopicWhenExists() throws Exception {
    when(mockAdminClient.describeTopics(eq(Collections.singleton("my-topic"))))
        .thenReturn(mockDescribeTopicsResult);
    Map<String, KafkaFuture<TopicDescription>> topicMap =
        Collections.singletonMap("my-topic", mockTopicDescFuture);
    when(mockDescribeTopicsResult.topicNameValues()).thenReturn(topicMap);
    when(mockTopicDescFuture.get()).thenReturn(null);

    ClusterSchema schema = new ClusterSchema(properties);
    Table table = schema.tables().get("my-topic");

    assertNotNull(table);
    assertInstanceOf(KafkaTopic.class, table);
  }

  @Test
  void tablesGetReturnsNullWhenTopicNotFound() throws Exception {
    when(mockAdminClient.describeTopics(eq(Collections.singleton("missing"))))
        .thenReturn(mockDescribeTopicsResult);
    Map<String, KafkaFuture<TopicDescription>> topicMap =
        Collections.singletonMap("missing", mockTopicDescFuture);
    when(mockDescribeTopicsResult.topicNameValues()).thenReturn(topicMap);
    when(mockTopicDescFuture.get()).thenThrow(
        new ExecutionException(new UnknownTopicOrPartitionException("not found")));

    ClusterSchema schema = new ClusterSchema(properties);
    Table table = schema.tables().get("missing");

    assertNull(table);
  }

  @Test
  void tablesGetRethrowsNonTopicException() throws Exception {
    when(mockAdminClient.describeTopics(eq(Collections.singleton("bad"))))
        .thenReturn(mockDescribeTopicsResult);
    Map<String, KafkaFuture<TopicDescription>> topicMap =
        Collections.singletonMap("bad", mockTopicDescFuture);
    when(mockDescribeTopicsResult.topicNameValues()).thenReturn(topicMap);
    when(mockTopicDescFuture.get()).thenThrow(
        new ExecutionException(new RuntimeException("auth error")));

    ClusterSchema schema = new ClusterSchema(properties);
    assertThrows(RuntimeException.class, () -> schema.tables().get("bad"));
  }

  @Test
  void tablesReturnsSameLookupInstance() {
    ClusterSchema schema = new ClusterSchema(properties);
    Lookup<Table> first = schema.tables();
    Lookup<Table> second = schema.tables();
    assertEquals(first, second);
  }
}
