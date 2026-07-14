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
import java.util.Collections;
import java.util.HashMap;
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
 * can fire on data availability: {@link #frontier(String)} reports the topic's event-time frontier
 * read via <em>this</em> cluster's connection {@code properties}, so every Kafka {@code Database}
 * reports a frontier from its own brokers with no global configuration.
 *
 * <p><b>Best-effort / lossy — a demo, not a correctness reference.</b> This source reports an
 * <em>optimistic</em> frontier (see below) but implements no repair ({@link #changesSince} is left
 * at its empty default), so data that lands behind the cursor is silently dropped. A source that
 * needs correctness must either report a conservative watermark or implement {@code changesSince};
 * see {@link InputFrontierSource}.
 */
public class ClusterSchema extends AbstractSchema implements InputFrontierSource {

  private static final Logger log = LoggerFactory.getLogger(ClusterSchema.class);

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
   * Reports the topic's event-time frontier as the <b>maximum</b> record timestamp across
   * partitions, via a single {@code listOffsets} call using {@link OffsetSpec#maxTimestamp()}
   * against this cluster's own {@code properties}. Returns empty when the topic is absent/empty or
   * the cluster can't be reached, so a trigger simply doesn't advance rather than failing.
   *
   * <p><b>This is optimistic and lossy.</b> Taking the max means the frontier advances to the
   * <em>fastest</em> partition, so records that later arrive on a lagging partition — or out of
   * order within a partition (Kafka {@code CreateTime} is not monotonic) — land behind the cursor
   * and, with no repair path here, are dropped. A production Kafka source would instead hold a
   * conservative watermark (min across partitions, minus a bounded out-of-orderness delay, with
   * idle-partition handling); this demo intentionally does not.
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
      long frontier = Long.MIN_VALUE;
      for (ListOffsetsResultInfo info : adminClient.listOffsets(query).all().get().values()) {
        if (info.timestamp() >= 0 && info.timestamp() > frontier) {
          frontier = info.timestamp();  // latest record timestamp across partitions
        }
      }
      return frontier == Long.MIN_VALUE ? Optional.empty() : Optional.of(Instant.ofEpochMilli(frontier));
    } catch (Exception e) {
      log.warn("Could not read Kafka frontier for topic {}: {}", topic, e.getMessage());
      return Optional.empty();
    }
  }

  /** Creates an {@link AdminClient} for this cluster. Package-private so tests can inject a mock. */
  AdminClient adminClient() {
    return AdminClient.create(properties);
  }
}
