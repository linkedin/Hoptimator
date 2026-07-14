package com.linkedin.hoptimator.kafka;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Integration test for {@link ClusterSchema}'s {@code InputWatermarkSource} capability against a live
 * Kafka cluster (the same one the {@code intTest} suite provisions). It creates a fresh topic,
 * produces records with known event-time timestamps across partitions, and asserts that
 * {@code watermark} reports the true frontier — the latest record timestamp across partitions —
 * via a real {@code describeTopics} + {@code listOffsets(maxTimestamp)} round-trip against the
 * cluster addressed by the schema's own {@code bootstrap.servers}.
 *
 * <p>The bootstrap servers default to {@code localhost:9092} (reachable from the test JVM via the
 * kind cluster's external listener) and can be overridden with the
 * {@code hoptimator.kafka.bootstrap.servers} system property.
 */
@Tag("integration")
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class ClusterSchemaWatermarkIntegrationTest {

  private static final String BOOTSTRAP_PROPERTY = "hoptimator.kafka.bootstrap.servers";
  private static final String DEFAULT_BOOTSTRAP = "localhost:9092";
  private static final int PARTITIONS = 2;

  private String bootstrap;
  private String topic;
  private AdminClient admin;

  @BeforeEach
  void setUp() throws Exception {
    bootstrap = System.getProperty(BOOTSTRAP_PROPERTY, DEFAULT_BOOTSTRAP);
    topic = "hoptimator-watermark-it-" + UUID.randomUUID();
    Properties adminProperties = new Properties();
    adminProperties.put("bootstrap.servers", bootstrap);
    admin = AdminClient.create(adminProperties);
    admin.createTopics(Collections.singletonList(new NewTopic(topic, PARTITIONS, (short) 1)))
        .all().get(30, TimeUnit.SECONDS);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (admin != null) {
      try {
        admin.deleteTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
      } finally {
        admin.close();
      }
    }
  }

  private ClusterSchema clusterSchema() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrap);
    return new ClusterSchema(properties);
  }

  @Test
  void reportsLatestRecordTimestampAcrossPartitionsFromLiveKafka() throws Exception {
    // Produce records with explicit, known event-time timestamps. The max across partitions is the
    // frontier the schema must report. Partition 0 gets the later timestamp so the answer is not
    // simply the last write.
    long base = System.currentTimeMillis();
    long partition0Timestamp = base - 10_000L;
    long partition1Timestamp = base - 60_000L;
    long expectedFrontier = Math.max(partition0Timestamp, partition1Timestamp);

    produce(0, partition0Timestamp);
    produce(1, partition1Timestamp);

    Optional<Instant> watermark = clusterSchema().watermark(topic);

    assertThat(watermark).contains(Instant.ofEpochMilli(expectedFrontier));
  }

  @Test
  void isEmptyForTopicWithNoRecords() {
    assertThat(clusterSchema().watermark(topic)).isEmpty();
  }

  @Test
  void isEmptyForNonExistentTopic() {
    assertThat(clusterSchema().watermark("hoptimator-watermark-absent-" + UUID.randomUUID())).isEmpty();
  }

  private void produce(int partition, long timestamp) throws Exception {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrap);
    properties.put("key.serializer", StringSerializer.class.getName());
    properties.put("value.serializer", StringSerializer.class.getName());
    try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
      producer.send(new ProducerRecord<>(topic, partition, timestamp, "k", "v")).get(30, TimeUnit.SECONDS);
      producer.flush();
    }
  }
}
