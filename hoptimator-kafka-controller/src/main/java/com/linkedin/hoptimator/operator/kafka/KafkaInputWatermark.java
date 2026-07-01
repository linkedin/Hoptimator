package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.operator.trigger.InputWatermarkProvider;
import com.linkedin.hoptimator.operator.trigger.TriggerInput;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;


/**
 * {@link InputWatermarkProvider} for Kafka topics: an event source that lets a {@code TableTrigger}
 * fire when new records arrive on a topic. It reports the topic's event-time frontier — the latest
 * record timestamp across partitions — via a single {@code AdminClient.listOffsets} call using
 * {@link OffsetSpec#maxTimestamp()}; the source-agnostic {@code TableTriggerReconciler} advances the
 * cursor to it and launches the job over the new window.
 *
 * <p>Kafka is an append-only stream, so completeness is monotone in record time; there is no
 * out-of-order partition repair, so {@link #changesSince} is left at its empty default.
 *
 * <p>The bootstrap servers are read from the {@code hoptimator.kafka.bootstrap.servers} system
 * property or the {@code KAFKA_BOOTSTRAP_SERVERS} environment variable. When neither is set the
 * provider is inactive (returns empty), so it never disturbs non-Kafka triggers.
 *
 * <p>Registered via {@code META-INF/services/com.linkedin.hoptimator.operator.trigger.InputWatermarkProvider}.
 */
public class KafkaInputWatermark implements InputWatermarkProvider {
  private static final Logger log = LoggerFactory.getLogger(KafkaInputWatermark.class);

  private static final String KAFKA_CATALOG = "KAFKA";
  private static final String BOOTSTRAP_PROPERTY = "hoptimator.kafka.bootstrap.servers";
  private static final String BOOTSTRAP_ENV = "KAFKA_BOOTSTRAP_SERVERS";

  private final boolean injected;
  private volatile AdminClient admin;

  public KafkaInputWatermark() {
    this.injected = false;
  }

  KafkaInputWatermark(AdminClient admin) {
    this.injected = true;
    this.admin = admin;
  }

  @Override
  public Optional<Instant> completeThrough(TriggerInput input) {
    if (!handles(input)) {
      return Optional.empty();
    }
    AdminClient client = admin();
    if (client == null) {
      return Optional.empty();
    }
    String topic = input.table();
    try {
      TopicDescription description =
          client.describeTopics(Collections.singleton(topic)).allTopicNames().get().get(topic);
      if (description == null) {
        return Optional.empty();
      }
      Map<TopicPartition, OffsetSpec> query = new HashMap<>();
      for (TopicPartitionInfo partition : description.partitions()) {
        query.put(new TopicPartition(topic, partition.partition()), OffsetSpec.maxTimestamp());
      }
      long frontier = Long.MIN_VALUE;
      for (ListOffsetsResultInfo info : client.listOffsets(query).all().get().values()) {
        if (info.timestamp() >= 0 && info.timestamp() > frontier) {
          frontier = info.timestamp();  // latest record timestamp across partitions
        }
      }
      return frontier == Long.MIN_VALUE ? Optional.empty() : Optional.of(Instant.ofEpochMilli(frontier));
    } catch (Exception e) {
      log.warn("Could not read Kafka watermark for topic {}: {}", topic, e.getMessage());
      return Optional.empty();
    }
  }

  private boolean handles(TriggerInput input) {
    if (input == null || input.table() == null) {
      return false;
    }
    return input.catalog() == null || KAFKA_CATALOG.equalsIgnoreCase(input.catalog());
  }

  private AdminClient admin() {
    if (admin == null && !injected) {
      synchronized (this) {
        if (admin == null) {
          String bootstrap = bootstrapServers();
          if (bootstrap == null || bootstrap.isEmpty()) {
            log.warn("Kafka bootstrap servers not configured ({} / {}); KafkaInputWatermark is inactive.",
                BOOTSTRAP_PROPERTY, BOOTSTRAP_ENV);
            return null;
          }
          Properties properties = new Properties();
          properties.put("bootstrap.servers", bootstrap);
          admin = AdminClient.create(properties);
        }
      }
    }
    return admin;
  }

  private static String bootstrapServers() {
    String value = System.getProperty(BOOTSTRAP_PROPERTY);
    if (value == null || value.isEmpty()) {
      value = System.getenv(BOOTSTRAP_ENV);
    }
    return value;
  }
}
