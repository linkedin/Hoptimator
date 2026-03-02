package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validated;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.jdbc.DeployerUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


/**
 * Deployer for Kafka topics. Creates topics in the synchronous DDL hot path.
 *
 * <p>Implements {@link Validated} to pre-check partition constraints
 * before any deployment side effects.
 */
public class KafkaDeployer implements Deployer, Validated {

  private static final Logger log = LoggerFactory.getLogger(KafkaDeployer.class);

  static final Integer DEFAULT_PARTITIONS = 10;
  static final Integer DEFAULT_REPLICATION_FACTOR = 3;
  static final String DEFAULT_RETENTION = String.valueOf(Duration.ofDays(7).toMillis());

  private final Source source;
  private final Properties properties;
  private boolean created = false;

  public KafkaDeployer(Source source, Properties properties) {
    this.source = source;
    this.properties = properties;
  }

  @Override
  public void validate(Validator.Issues issues) {
    String topicName = source.table();
    // null default = option was not specified by user, skip validation for that option
    Integer partitions = DeployerUtils.parseIntOption(source.options(), "partitions", null);
    Integer replicationFactor = DeployerUtils.parseIntOption(source.options(), "replicationFactor", null);

    if (partitions != null && partitions <= 0) {
      issues.error("Partition count must be positive, got: " + partitions);
    }
    if (replicationFactor != null && replicationFactor <= 0) {
      issues.error("Replication factor must be positive, got: " + replicationFactor);
    }

    // Describe existing topic to check partition decrease constraint
    if (partitions != null) {
      try (AdminClient admin = AdminClient.create(properties)) {
        DescribeTopicsResult rs = admin.describeTopics(Collections.singletonList(topicName));
        TopicDescription topic = rs.topicNameValues().get(topicName).get();

        int actualPartitions = topic.partitions().size();
        if (partitions < actualPartitions) {
          issues.error("Cannot decrease partitions from " + actualPartitions + " to " + partitions
                  + " for topic " + topicName + ". Partition decrease is not supported.");
        }
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
          issues.error("Failed to describe existing topic " + topicName + ": " + e.getMessage());
        }
        // UnknownTopicOrPartitionException is fine — topic doesn't exist yet, will be created
      } catch (Exception e) {
        issues.error("Failed to validate topic " + topicName + ": " + e.getMessage());
      }
    }
  }

  @Override
  public void create() throws SQLException {
    String topicName = source.table();
    int partitions = DeployerUtils.parseIntOption(source.options(), "partitions", DEFAULT_PARTITIONS);
    short replicationFactor = DeployerUtils.parseIntOption(source.options(), "replicationFactor", DEFAULT_REPLICATION_FACTOR).shortValue();
    @Nullable Long retention = DeployerUtils.parseLongOption(source.options(), "retention", null);

    try (AdminClient admin = AdminClient.create(properties)) {
      if (topicExists(admin, topicName)) {
        log.info("Kafka topic {} already exists, skipping creation", topicName);
        return;
      }
      createTopic(admin, topicName, partitions, replicationFactor, retention);
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException("Failed to create topic " + topicName, e);
    }
  }

  @Override
  public void delete() throws SQLException {
    String topicName = source.table();
    try (AdminClient admin = AdminClient.create(properties)) {
      log.info("Deleting Kafka topic {}...", topicName);
      admin.deleteTopics(Collections.singletonList(topicName)).all().get();
    } catch (Exception e) {
      throw new SQLException("Failed to delete topic " + topicName, e);
    }
  }

  @Override
  public void update() throws SQLException {
    String topicName = source.table();
    int partitions = DeployerUtils.parseIntOption(source.options(), "partitions", DEFAULT_PARTITIONS);
    short replicationFactor = DeployerUtils.parseIntOption(source.options(), "replicationFactor", DEFAULT_REPLICATION_FACTOR).shortValue();
    @Nullable Long retention = DeployerUtils.parseLongOption(source.options(), "retention", null);

    try (AdminClient admin = AdminClient.create(properties)) {
      TopicDescription topic = describeTopic(admin, topicName);

      if (topic != null) {
        // Topic exists, update it
        updateExistingTopic(admin, topicName, topic, partitions, retention);
      } else {
        // Topic doesn't exist, create it
        log.info("Topic {} does not exist. Creating with partitions={}, replicationFactor={}, retention={}",
            topicName, partitions, replicationFactor, retention != null ? retention : DEFAULT_RETENTION);
        createTopic(admin, topicName, partitions, replicationFactor, retention);
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      throw new SQLException("Failed to update topic " + topicName, e);
    }
  }

  @Override
  public List<String> specify() throws SQLException {
    return Collections.emptyList();
  }

  @Override
  public void restore() {
    if (!created) {
      return;
    }
    String topicName = source.table();
    log.warn("Rollback requested for topic {}. The topic may need to be rolled back manually.", topicName);
  }

  /**
   * Checks if a Kafka topic exists.
   * @return true if topic exists, false if it doesn't exist
   * @throws SQLException if there's an error checking (other than topic not found)
   */
  private boolean topicExists(AdminClient admin, String topicName) throws SQLException {
    try {
      DescribeTopicsResult rs = admin.describeTopics(Collections.singletonList(topicName));
      rs.topicNameValues().get(topicName).get();
      return true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        return false;
      }
      throw new SQLException("Failed to check if topic exists: " + topicName, e);
    } catch (Exception e) {
      throw new SQLException("Failed to check if topic exists: " + topicName, e);
    }
  }

  /**
   * Describes a Kafka topic.
   * @return TopicDescription if topic exists, null if it doesn't exist
   * @throws SQLException if there's an error describing the topic (other than topic not found)
   */
  private TopicDescription describeTopic(AdminClient admin, String topicName) throws SQLException {
    try {
      log.info("Describing Kafka topic {}...", topicName);
      DescribeTopicsResult rs = admin.describeTopics(Collections.singletonList(topicName));
      return rs.topicNameValues().get(topicName).get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        return null;
      }
      throw new SQLException("Failed to describe topic " + topicName, e);
    } catch (Exception e) {
      throw new SQLException("Failed to describe topic " + topicName, e);
    }
  }

  /**
   * Creates a new Kafka topic.
   */
  private void createTopic(AdminClient admin, String topicName, int partitions,
      short replicationFactor, @Nullable Long retention) throws Exception {
    log.info("Creating Kafka topic {} with partitions={}, replicationFactor={}, retention={}",
        topicName, partitions, replicationFactor, retention != null ? retention : DEFAULT_RETENTION);
    Map<String, String> configs = new HashMap<>();
    configs.put(TopicConfig.RETENTION_MS_CONFIG,
        retention != null ? String.valueOf(retention) : DEFAULT_RETENTION);
    NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor).configs(configs);
    admin.createTopics(Collections.singletonList(newTopic)).all().get();
    created = true;
    log.info("Successfully created topic {}", topicName);
  }

  /**
   * Updates an existing Kafka topic (increases partitions and/or changes retention).
   */
  private void updateExistingTopic(AdminClient admin, String topicName, TopicDescription topic,
      int partitions, @Nullable Long retention) throws Exception {
    log.info("Topic {} already exists with {} partitions", topicName, topic.partitions().size());
    int actualPartitions = topic.partitions().size();

    if (partitions > actualPartitions) {
      log.info("Increasing partitions from {} to {} for topic {}", actualPartitions, partitions, topicName);
      admin.createPartitions(Map.of(topicName, NewPartitions.increaseTo(partitions))).all().get();
    }

    if (retention != null) {
      log.info("Changing retention for topic {} to {}", topicName, retention);
      ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
      AlterConfigOp setRetentionOp = new AlterConfigOp(
          new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retention)),
          AlterConfigOp.OpType.SET
      );
      admin.incrementalAlterConfigs(
          Collections.singletonMap(topicResource, Collections.singletonList(setRetentionOp))).all().get();
    }

    log.info("Successfully updated topic {}", topicName);
  }
}
