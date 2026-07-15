package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.InputFrontierSource;
import com.linkedin.hoptimator.jdbc.schema.LazyLookup;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;


/**
 * Schema for Kafka topics with lazy loading.
 * Tables are loaded on-demand when first accessed, not during driver connection.
 *
 * <p>Implements {@link InputFrontierSource} so a {@code TableTrigger} over a topic in this cluster
 * can fire on data availability: {@link #frontier(String)} reports the topic's event-time
 * completeness watermark, read via <em>this</em> cluster's connection {@code properties}, so every
 * Kafka {@code Database} watermarks against its own brokers with no global configuration.
 *
 * <p><b>Bounded out-of-orderness watermark.</b> The watermark is the per-partition minimum of
 * {@code maxTimestamp}, held back by a lag {@code B} (the {@code frontier.lag.ms} connection
 * property, default 5 minutes), excluding partitions that lag more than {@code B} behind the leader
 * (idle/straggler detection). It is complete for records arriving within {@code B} of event-time
 * order; a partition that falls more than {@code B} behind the leader is treated as idle, so it
 * never stalls the topic — at the cost that data it later emits below the watermark may be dropped.
 * There is no repair path ({@link #changesSince} is left at its empty default): completeness comes
 * from the conservative watermark itself.
 */
public class ClusterSchema extends AbstractSchema implements InputFrontierSource {

  private static final Logger log = LoggerFactory.getLogger(ClusterSchema.class);

  /** Per-database out-of-orderness / idle bound {@code B}, in milliseconds. */
  static final String FRONTIER_LAG_MS_PROPERTY = "frontier.lag.ms";
  static final long DEFAULT_FRONTIER_LAG_MS = 5 * 60 * 1000L;

  private final Properties properties;
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public ClusterSchema(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyLookup<>() {

      @Override
      protected Map<String, Table> loadAll() throws Exception {
        try (AdminClient adminClient = adminClient()) {
          Set<String> topicNames = adminClient.listTopics().names().get();
          Map<String, Table> tables = new HashMap<>();
          for (String topicName : topicNames) {
            tables.put(topicName, new KafkaTopic(topicName, properties));
          }
          return tables;
        }
      }

      @Override
      protected @Nullable Table load(String name) throws Exception {
        try (AdminClient adminClient = adminClient()) {
          // Attempt to get the topic description, which will throw an exception if it doesn't exist
          adminClient.describeTopics(Collections.singleton(name)).topicNameValues().get(name).get();
          return new KafkaTopic(name, properties);
        } catch (ExecutionException e) {
          // Check the underlying cause of the exception
          if (e.getCause() instanceof UnknownTopicOrPartitionException) {
            return null;
          }
          throw e;
        }
      }

      @Override
      protected String getDescription() {
        return "Kafka at " + properties.getProperty("bootstrap.servers");
      }
    });
  }

  /**
   * Reports the topic's event-time completeness watermark: the per-partition minimum of
   * {@code maxTimestamp}, excluding partitions more than {@code B} behind the leader, then held back
   * by {@code B} (bounded out-of-orderness). Returns empty when the topic is absent/empty or the
   * cluster can't be reached, so a trigger simply doesn't advance rather than failing.
   *
   * <p>Concretely, with per-partition maxima and leader {@code M = max}: drop any partition below
   * {@code M - B} (idle/straggler), take the {@code min} of the rest, and subtract {@code B}. This
   * is complete for records arriving within {@code B} of event-time order; idle/empty partitions
   * never stall the topic.
   */
  @Override
  public Optional<Instant> frontier(String topic) {
    if (topic == null) {
      return Optional.empty();
    }
    try (AdminClient adminClient = adminClient()) {
      TopicDescription description =
          adminClient.describeTopics(Collections.singleton(topic)).allTopicNames().get().get(topic);
      if (description == null) {
        return Optional.empty();
      }
      Map<TopicPartition, OffsetSpec> query = new HashMap<>();
      for (TopicPartitionInfo partition : description.partitions()) {
        query.put(new TopicPartition(topic, partition.partition()), OffsetSpec.maxTimestamp());
      }
      List<Long> partitionMax = new ArrayList<>();
      for (ListOffsetsResultInfo info : adminClient.listOffsets(query).all().get().values()) {
        if (info.timestamp() >= 0) {
          partitionMax.add(info.timestamp());  // latest record timestamp in this partition
        }
      }
      if (partitionMax.isEmpty()) {
        return Optional.empty();  // no records anywhere yet
      }

      long lagMs = lagMs();
      long leader = Collections.max(partitionMax);
      // Exclude partitions more than B behind the leader (idle/straggler), so a lagging or dead
      // partition never stalls the topic; take the min of the rest and hold back by B so records
      // arriving within B of event-time order are still ahead of the cursor.
      long minKeepingUp = Long.MAX_VALUE;
      for (long max : partitionMax) {
        if (max >= leader - lagMs && max < minKeepingUp) {
          minKeepingUp = max;
        }
      }
      return Optional.of(Instant.ofEpochMilli(minKeepingUp - lagMs));
    } catch (Exception e) {
      log.warn("Could not read Kafka frontier for topic {}: {}", topic, e.getMessage());
      return Optional.empty();
    }
  }

  private long lagMs() {
    String configured = properties.getProperty(FRONTIER_LAG_MS_PROPERTY);
    if (configured == null || configured.isEmpty()) {
      return DEFAULT_FRONTIER_LAG_MS;
    }
    try {
      return Long.parseLong(configured.trim());
    } catch (NumberFormatException e) {
      log.warn("Invalid {}='{}'; using default {}ms.", FRONTIER_LAG_MS_PROPERTY, configured, DEFAULT_FRONTIER_LAG_MS);
      return DEFAULT_FRONTIER_LAG_MS;
    }
  }

  /** Creates an {@link AdminClient} for this cluster. Package-private so tests can inject a mock. */
  AdminClient adminClient() {
    // Strip non-Kafka control properties (e.g. frontier.lag.ms) so the AdminClient doesn't warn.
    Properties adminProperties = new Properties();
    for (String name : properties.stringPropertyNames()) {
      if (!name.startsWith("frontier.")) {
        adminProperties.setProperty(name, properties.getProperty(name));
      }
    }
    return AdminClient.create(adminProperties);
  }
}
