package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
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

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.mockito.ArgumentCaptor;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class KafkaDeployerTest {

  private static final Properties PROPERTIES = new Properties();

  @Mock
  private AdminClient mockAdmin;

  @Mock
  private MockedStatic<AdminClient> adminClientStatic;

  @BeforeEach
  void setUp() {
    adminClientStatic.when(() -> AdminClient.create(any(Properties.class))).thenReturn(mockAdmin);
  }

  private KafkaDeployer createDeployer(Source source) {
    return new KafkaDeployer(source, PROPERTIES);
  }

  // --- create()/update() tests ---

  @SuppressWarnings("unchecked")
  @Test
  void testCreateNewTopic() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "NewTopic"), Collections.emptyMap());

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("not found")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("NewTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createTopics(anyList())).thenReturn(createResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.create();

    verify(mockAdmin).createTopics(anyList());
    verify(mockAdmin).close();
  }

  @Test
  void testUpdateExistingTopicSkipsCreation() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "ExistingTopic"), Collections.emptyMap());

    TopicDescription topicDesc = mockTopicWithPartitions(10);
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> future = KafkaFuture.completedFuture(topicDesc);

    when(describeResult.topicNameValues()).thenReturn(Map.of("ExistingTopic", future));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.update();

    verify(mockAdmin, never()).createTopics(anyList());
    verify(mockAdmin, never()).createPartitions(any());
    verify(mockAdmin, never()).incrementalAlterConfigs(any());
    verify(mockAdmin).close();
  }

  @Test
  void testCreateExistingTopicIncreasesPartitions() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "GrowingTopic"),
        Map.of("partitions", "15"));

    TopicDescription topicDesc = mockTopicWithPartitions(10);
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> future = KafkaFuture.completedFuture(topicDesc);

    when(describeResult.topicNameValues()).thenReturn(Map.of("GrowingTopic", future));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    CreatePartitionsResult partitionsResult = mock(CreatePartitionsResult.class);
    KafkaFuture<Void> partitionsFuture = KafkaFuture.completedFuture(null);
    when(partitionsResult.all()).thenReturn(partitionsFuture);
    when(mockAdmin.createPartitions(any())).thenReturn(partitionsResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.update();

    verify(mockAdmin, never()).createTopics(anyList());
    verify(mockAdmin).createPartitions(any());
    verify(mockAdmin, never()).incrementalAlterConfigs(any());
  }

  @Test
  void testCreateExistingTopicChangeRetention() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "RetentionTopic"),
        Map.of("retention", "604800000"));

    TopicDescription topicDesc = mockTopicWithPartitions(10);
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> future = KafkaFuture.completedFuture(topicDesc);

    when(describeResult.topicNameValues()).thenReturn(Map.of("RetentionTopic", future));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    AlterConfigsResult alterResult = mock(AlterConfigsResult.class);
    KafkaFuture<Void> alterFuture = KafkaFuture.completedFuture(null);
    when(alterResult.all()).thenReturn(alterFuture);
    when(mockAdmin.incrementalAlterConfigs(any())).thenReturn(alterResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.update();

    verify(mockAdmin, never()).createTopics(anyList());
    verify(mockAdmin, never()).createPartitions(any());
    verify(mockAdmin).incrementalAlterConfigs(any());
  }

  @Test
  void testCreateExistingTopicDoesNotDecreasePartitions() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "ShrinkTopic"),
        Map.of("partitions", "5"));

    TopicDescription topicDesc = mockTopicWithPartitions(10);
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> future = KafkaFuture.completedFuture(topicDesc);

    when(describeResult.topicNameValues()).thenReturn(Map.of("ShrinkTopic", future));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.update();

    verify(mockAdmin, never()).createPartitions(any());
    verify(mockAdmin, never()).createTopics(anyList());
    verify(mockAdmin, never()).incrementalAlterConfigs(any());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testCreateWithCustomPartitionsAndReplicationFactor() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "CustomTopic"),
        Map.of("partitions", "32", "replicationFactor", "5"));

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("not found")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("CustomTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createTopics(anyList())).thenReturn(createResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.create();

    verify(mockAdmin).createTopics(anyList());
  }

  @SuppressWarnings("unchecked")
  @Test
  void testCreatePropagatesNonTopicNotFoundError() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "ErrorTopic"), Collections.emptyMap());

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new RuntimeException("connection refused")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("ErrorTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    assertThrows(SQLException.class, deployer::create);
  }

  // --- validate() tests ---

  @Test
  void testValidatePassesForNewTopic() {
    // No partitions option => validate() returns immediately without calling AdminClient
    Source source = new Source("db", List.of("KAFKA", "NewTopic"), Collections.emptyMap());

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertTrue(issues.valid(), "Expected no validation errors for new topic");
  }

  @Test
  void testValidateRejectsPartitionDecrease() {
    Source source = new Source("db", List.of("KAFKA", "ShrinkTopic"),
        Map.of("partitions", "5"));

    TopicDescription topicDesc = mockTopicWithPartitions(10);
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> future = KafkaFuture.completedFuture(topicDesc);

    when(describeResult.topicNameValues()).thenReturn(Map.of("ShrinkTopic", future));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertFalse(issues.valid(), "Expected validation error for partition decrease");
    assertTrue(issues.toString().contains("Cannot decrease partitions"));
  }

  @Test
  void testValidateRejectsZeroPartitions() {
    Source source = new Source("db", List.of("KAFKA", "BadTopic"),
        Map.of("partitions", "0"));

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("must be positive"));
  }

  @Test
  void testValidatePassesWhenNoPartitionOptionAndTopicExists() {
    Source source = new Source("db", List.of("KAFKA", "ExistingBigTopic"), Collections.emptyMap());

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertTrue(issues.valid(),
        "Validation should pass when no partition count is specified");
  }

  @Test
  void testValidateAllowsPartitionIncrease() {
    Source source = new Source("db", List.of("KAFKA", "GrowTopic"),
        Map.of("partitions", "20"));

    TopicDescription topicDesc = mockTopicWithPartitions(10);
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> future = KafkaFuture.completedFuture(topicDesc);

    when(describeResult.topicNameValues()).thenReturn(Map.of("GrowTopic", future));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertTrue(issues.valid(), "Partition increase should be allowed");
  }

  // --- restore() tests ---

  @Test
  void testRestoreNoOpWhenNotCreated() {
    Source source = new Source("db", List.of("KAFKA", "TestTopic"), Collections.emptyMap());
    KafkaDeployer deployer = createDeployer(source);

    deployer.restore();
  }

  @SuppressWarnings("unchecked")
  @Test
  void testRestoreLogsWarningAfterCreate() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "CreatedTopic"), Collections.emptyMap());

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("not found")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("CreatedTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createTopics(anyList())).thenReturn(createResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.create();
    deployer.restore();
  }

  // --- delete() tests ---

  @Test
  void testDeleteTopic() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "TopicToDelete"), Collections.emptyMap());

    DeleteTopicsResult deleteResult = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> deleteFuture = KafkaFuture.completedFuture(null);
    when(deleteResult.all()).thenReturn(deleteFuture);
    when(mockAdmin.deleteTopics(anyList())).thenReturn(deleteResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.delete();

    verify(mockAdmin).deleteTopics(anyList());
    verify(mockAdmin).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  void testDeleteTopicThrowsException() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "ErrorTopic"), Collections.emptyMap());

    DeleteTopicsResult deleteResult = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> deleteFuture = mock(KafkaFuture.class);
    when(deleteFuture.get()).thenThrow(new ExecutionException(new RuntimeException("Delete failed")));
    when(deleteResult.all()).thenReturn(deleteFuture);
    when(mockAdmin.deleteTopics(anyList())).thenReturn(deleteResult);

    KafkaDeployer deployer = createDeployer(source);
    SQLException exception = assertThrows(SQLException.class, deployer::delete);

    assertTrue(exception.getMessage().contains("Failed to delete topic ErrorTopic"));
  }

  @SuppressWarnings("unchecked")
  @Test
  void testDeleteNonExistentTopic() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "NonExistentTopic"), Collections.emptyMap());

    DeleteTopicsResult deleteResult = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> deleteFuture = mock(KafkaFuture.class);
    when(deleteFuture.get()).thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("Topic not found")));
    when(deleteResult.all()).thenReturn(deleteFuture);
    when(mockAdmin.deleteTopics(anyList())).thenReturn(deleteResult);

    KafkaDeployer deployer = createDeployer(source);
    SQLException exception = assertThrows(SQLException.class, deployer::delete);

    assertTrue(exception.getMessage().contains("Failed to delete topic NonExistentTopic"));
  }

  // --- specify() tests ---

  @Test
  void testSpecifyReturnsEmptyList() throws SQLException {
    Source source = new Source("db", List.of("KAFKA", "TestTopic"), Collections.emptyMap());
    KafkaDeployer deployer = createDeployer(source);

    assertTrue(deployer.specify().isEmpty());
  }

  @Test
  void testValidateWithNegativeReplicationFactor() {
    Source source = new Source("db", List.of("KAFKA", "BadTopic"),
        Map.of("replicationFactor", "-1"));

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("must be positive"));
  }

  // --- validate() error path tests ---

  @Test
  void testValidateAdminClientThrowsGenericException() {
    Source source = new Source("db", List.of("KAFKA", "BrokenTopic"),
        Map.of("partitions", "10"));

    when(mockAdmin.describeTopics(anyList())).thenThrow(new RuntimeException("admin client broken"));

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Failed to validate topic"));
  }

  @SuppressWarnings("unchecked")
  @Test
  void testValidateAdminClientThrowsNonTopicNotFound() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "AuthFailTopic"),
        Map.of("partitions", "10"));

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new RuntimeException("auth failed")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("AuthFailTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Failed to describe existing topic"));
  }

  // --- topicExists() coverage ---

  @Test
  void testCreateExistingTopicSkipsCreation() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "ExistingTopic"), Collections.emptyMap());

    TopicDescription topicDesc = mockTopicWithPartitions(10);
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> future = KafkaFuture.completedFuture(topicDesc);

    when(describeResult.topicNameValues()).thenReturn(Map.of("ExistingTopic", future));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.create();

    verify(mockAdmin, never()).createTopics(anyList());
    verify(mockAdmin).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  void testCreateTopicExistsCheckThrowsGenericException() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "BrokenTopic"), Collections.emptyMap());

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new InterruptedException("interrupted"));
    when(describeResult.topicNameValues()).thenReturn(Map.of("BrokenTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    SQLException exception = assertThrows(SQLException.class, deployer::create);

    assertTrue(exception.getMessage().contains("Failed to check if topic exists: BrokenTopic"));
  }

  // --- describeTopic() coverage via update() ---

  @SuppressWarnings("unchecked")
  @Test
  void testUpdateCreatesTopicWhenNotExists() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "MissingTopic"), Collections.emptyMap());

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("not found")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("MissingTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createTopics(anyList())).thenReturn(createResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.update();

    verify(mockAdmin).createTopics(anyList());
    verify(mockAdmin).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  void testUpdateDescribeTopicThrowsNonTopicException() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "AuthFailTopic"), Collections.emptyMap());

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new RuntimeException("auth failed")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("AuthFailTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    SQLException exception = assertThrows(SQLException.class, deployer::update);

    assertTrue(exception.getMessage().contains("Failed to describe topic AuthFailTopic"));
  }

  @SuppressWarnings("unchecked")
  @Test
  void testUpdateDescribeTopicThrowsGenericException() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "BrokenTopic"), Collections.emptyMap());

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new InterruptedException("interrupted"));
    when(describeResult.topicNameValues()).thenReturn(Map.of("BrokenTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    SQLException exception = assertThrows(SQLException.class, deployer::update);

    assertTrue(exception.getMessage().contains("Failed to describe topic BrokenTopic"));
  }

  // --- update() with custom retention on new topic ---

  @SuppressWarnings("unchecked")
  @Test
  void testUpdateCreatesTopicWithCustomRetention() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "RetentionNewTopic"),
        Map.of("retention", "86400000"));

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("not found")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("RetentionNewTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createTopics(anyList())).thenReturn(createResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.update();

    verify(mockAdmin).createTopics(anyList());
  }

  // --- create() with custom retention ---

  @SuppressWarnings("unchecked")
  @Test
  void testCreateNewTopicWithRetention() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "RetentionTopic"),
        Map.of("retention", "86400000"));

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("not found")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("RetentionTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createTopics(anyList())).thenReturn(createResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.create();

    verify(mockAdmin).createTopics(anyList());
  }

  // --- helpers ---

  private TopicDescription mockTopicWithPartitions(int numPartitions) {
    TopicDescription topicDesc = mock(TopicDescription.class);
    List<TopicPartitionInfo> partitions = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(mock(TopicPartitionInfo.class));
    }
    // lenient: some callers only need existence, not partition count
    lenient().when(topicDesc.partitions()).thenReturn(partitions);
    return topicDesc;
  }

  private Validator.Issues collectIssues(KafkaDeployer deployer) {
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);
    return issues;
  }

  // --- createTopic() retention config assertion ---

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  void testCreateTopicWithCustomRetentionPassesRetentionToNewTopic() throws Exception {
    long customRetention = 86400000L;
    Source source = new Source("db", List.of("KAFKA", "RetentionConfigTopic"),
        Map.of("retention", String.valueOf(customRetention)));

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("not found")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("RetentionConfigTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createTopics(anyList())).thenReturn(createResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.create();

    ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
    verify(mockAdmin).createTopics(captor.capture());
    NewTopic newTopic = (NewTopic) captor.getValue().iterator().next();
    assertEquals(String.valueOf(customRetention), newTopic.configs().get(TopicConfig.RETENTION_MS_CONFIG),
        "Custom retention should be set in NewTopic configs");
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  void testCreateTopicWithDefaultRetentionUsesDefaultRetentionValue() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "DefaultRetentionTopic"), Collections.emptyMap());

    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> failedFuture = mock(KafkaFuture.class);
    when(failedFuture.get()).thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("not found")));
    when(describeResult.topicNameValues()).thenReturn(Map.of("DefaultRetentionTopic", failedFuture));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    CreateTopicsResult createResult = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createFuture = KafkaFuture.completedFuture(null);
    when(createResult.all()).thenReturn(createFuture);
    when(mockAdmin.createTopics(anyList())).thenReturn(createResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.create();

    ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
    verify(mockAdmin).createTopics(captor.capture());
    NewTopic newTopic = (NewTopic) captor.getValue().iterator().next();
    String expectedDefault = String.valueOf(Duration.ofDays(7).toMillis());
    assertEquals(expectedDefault, newTopic.configs().get(TopicConfig.RETENTION_MS_CONFIG),
        "Default retention (7 days ms) should be set when retention option is absent");
  }

  @Test
  void testRestoreWithoutCreateDoesNotCallAdmin() {
    Source source = new Source("db", List.of("KAFKA", "NeverCreated"), Collections.emptyMap());
    KafkaDeployer deployer = createDeployer(source);

    // restore() when created==false must be a no-op: no AdminClient interaction
    deployer.restore();

    // `if (!created) return` was removed, the code would attempt to log
    // a warning (which itself doesn't call admin), but the guard is the distinction between
    // "never created" and "was created". Verify admin was not used at all.
    verify(mockAdmin, never()).deleteTopics(anyList());
    verify(mockAdmin, never()).close();
  }

  // --- update() equal partitions ---

  @Test
  void testUpdateWithEqualPartitionsDoesNotAlterPartitions() throws Exception {
    Source source = new Source("db", List.of("KAFKA", "SamePartsTopic"),
        Map.of("partitions", "10"));

    TopicDescription topicDesc = mockTopicWithPartitions(10);
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> future = KafkaFuture.completedFuture(topicDesc);

    when(describeResult.topicNameValues()).thenReturn(Map.of("SamePartsTopic", future));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    deployer.update();

    verify(mockAdmin, never()).createPartitions(any());
    verify(mockAdmin, never()).createTopics(anyList());
  }

  // --- validate() boundary tests ---

  @Test
  void testValidatePassesWithExactlyOnePartition() {
    Source source = new Source("db", List.of("KAFKA", "OnePart"),
        Map.of("partitions", "1"));

    TopicDescription topicDesc = mockTopicWithPartitions(1);
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> future = KafkaFuture.completedFuture(topicDesc);

    when(describeResult.topicNameValues()).thenReturn(Map.of("OnePart", future));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertTrue(issues.valid(), "Exactly 1 partition should be valid");
  }

  @Test
  void testValidateRejectsZeroReplicationFactor() {
    Source source = new Source("db", List.of("KAFKA", "ZeroReplTopic"),
        Map.of("replicationFactor", "0"));

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("must be positive"),
        "Zero replication factor should fail validation");
  }

  @Test
  void testValidatePassesWithExactlyOneReplicationFactor() {
    Source source = new Source("db", List.of("KAFKA", "OneReplTopic"),
        Map.of("replicationFactor", "1"));

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertTrue(issues.valid(), "Exactly 1 replication factor should be valid (no partitions check needed)");
  }

  @Test
  void testValidatePassesWhenPartitionsEqualsActual() {
    // partitions == actualPartitions: NOT a decrease, should be valid
    Source source = new Source("db", List.of("KAFKA", "EqualPartsTopic"),
        Map.of("partitions", "5"));

    TopicDescription topicDesc = mockTopicWithPartitions(5);
    DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> future = KafkaFuture.completedFuture(topicDesc);

    when(describeResult.topicNameValues()).thenReturn(Map.of("EqualPartsTopic", future));
    when(mockAdmin.describeTopics(anyList())).thenReturn(describeResult);

    KafkaDeployer deployer = createDeployer(source);
    Validator.Issues issues = collectIssues(deployer);

    assertTrue(issues.valid(), "partitions == actualPartitions should NOT be flagged as a decrease");
  }
}
