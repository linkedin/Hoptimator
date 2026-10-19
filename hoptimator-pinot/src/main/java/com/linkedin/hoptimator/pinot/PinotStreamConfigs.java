package com.linkedin.hoptimator.pinot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;

/**
 * Builds and validates the Pinot {@code streamConfigs} map for a REALTIME table's Kafka ingestion,
 * driven by connector hints. This is the streaming analogue of {@link PinotSchemas}: it centralizes
 * the hint vocabulary and reuses Pinot's own {@link StreamConfig} constructor for validation, so a
 * bad spec cannot pass through and create a broken table.
 *
 * <p>Hint vocabulary:
 * <ul>
 *   <li>{@code kafkaTopic} — source topic name (required)</li>
 *   <li>{@code kafkaBrokerList} — comma-separated {@code host:port} bootstrap brokers (required)</li>
 *   <li>{@code streamType} — stream type (default {@code kafka})</li>
 *   <li>{@code streamConsumerType} — {@code lowlevel} (default) or {@code highlevel}</li>
 *   <li>{@code streamConsumerFactoryClass} — consumer factory class (default Pinot's Kafka factory)</li>
 *   <li>{@code streamDecoderClass} — message decoder class (default the Kafka JSON decoder)</li>
 *   <li>{@code streamOffsetCriteria} — {@code smallest} (default) / {@code largest}</li>
 *   <li>{@code flushThresholdRows} — optional realtime segment flush threshold (rows)</li>
 * </ul>
 *
 * <p>This is a generic Apache Pinot base: it defaults to the standard Kafka consumer factory and
 * JSON decoder. A control-plane that synthesizes its own stream config server-side does not use
 * this helper.
 *
 * <p>TODO(known gaps): the decoder defaults to the Kafka JSON decoder. Avro / Confluent-schema-registry
 * decoders are reachable via {@code streamDecoderClass} (+ {@code decoder.prop.*} would need
 * threading), but are not first-class here yet — add explicit support + defaults if the common
 * case becomes Avro. Related: an advisory pre-flight that diffs the topic's registered schema against
 * the Pinot schema is deliberately out of scope (Pinot itself reconciles by name at ingestion).
 */
public final class PinotStreamConfigs {

  public static final String KAFKA_TOPIC = "kafkaTopic";
  public static final String KAFKA_BROKER_LIST = "kafkaBrokerList";
  public static final String STREAM_TYPE = "streamType";
  public static final String CONSUMER_TYPE = "streamConsumerType";
  public static final String CONSUMER_FACTORY_CLASS = "streamConsumerFactoryClass";
  public static final String DECODER_CLASS = "streamDecoderClass";
  public static final String OFFSET_CRITERIA = "streamOffsetCriteria";
  public static final String FLUSH_THRESHOLD_ROWS = "flushThresholdRows";

  public static final String DEFAULT_STREAM_TYPE = "kafka";
  public static final String DEFAULT_CONSUMER_TYPE = "lowlevel";
  public static final String DEFAULT_DECODER_CLASS =
      "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder";
  public static final String DEFAULT_OFFSET_CRITERIA = "smallest";

  // Kafka-plugin-specific suffixes (not in StreamConfigProperties, which only defines stream-agnostic keys).
  private static final String BROKER_LIST_SUFFIX = "broker.list";
  private static final String CONSUMER_TYPE_SUFFIX = "consumer.type";

  private PinotStreamConfigs() {
  }

  /** Builds the {@code streamConfigs} map (with defaults) for the table's Kafka ingestion. */
  public static Map<String, String> build(Map<String, String> hints) {
    String streamType = orDefault(hints.get(STREAM_TYPE), DEFAULT_STREAM_TYPE);
    Map<String, String> configs = new LinkedHashMap<>();
    configs.put(StreamConfigProperties.STREAM_TYPE, streamType);
    putStreamProperty(configs, streamType, StreamConfigProperties.STREAM_TOPIC_NAME, hints.get(KAFKA_TOPIC));
    putStreamProperty(configs, streamType, BROKER_LIST_SUFFIX, hints.get(KAFKA_BROKER_LIST));
    putStreamProperty(configs, streamType, CONSUMER_TYPE_SUFFIX,
        orDefault(hints.get(CONSUMER_TYPE), DEFAULT_CONSUMER_TYPE));
    putStreamProperty(configs, streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS,
        orDefault(hints.get(CONSUMER_FACTORY_CLASS), StreamConfig.DEFAULT_CONSUMER_FACTORY_CLASS_NAME_STRING));
    putStreamProperty(configs, streamType, StreamConfigProperties.STREAM_DECODER_CLASS,
        orDefault(hints.get(DECODER_CLASS), DEFAULT_DECODER_CLASS));
    putStreamProperty(configs, streamType, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA,
        orDefault(hints.get(OFFSET_CRITERIA), DEFAULT_OFFSET_CRITERIA));
    if (!isBlank(hints.get(FLUSH_THRESHOLD_ROWS))) {
      configs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, hints.get(FLUSH_THRESHOLD_ROWS).trim());
    }
    return configs;
  }

  /**
   * Returns the reasons the REALTIME stream config is invalid, or an empty list when it is valid.
   * Checks the required topic/broker hints, then delegates to Pinot's own {@link StreamConfig}
   * constructor (which enforces the required stream properties: type, topic, consumer factory, decoder).
   */
  public static List<String> validationErrors(String tableName, Map<String, String> hints) {
    List<String> errors = new ArrayList<>();
    if (isBlank(hints.get(KAFKA_TOPIC))) {
      errors.add("REALTIME Pinot table " + tableName + " requires a 'kafkaTopic' hint.");
    }
    if (isBlank(hints.get(KAFKA_BROKER_LIST))) {
      errors.add("REALTIME Pinot table " + tableName + " requires a 'kafkaBrokerList' hint.");
    }
    if (errors.isEmpty()) {
      try {
        new StreamConfig(tableName, build(hints));
      } catch (RuntimeException e) {
        errors.add("Invalid stream config for REALTIME Pinot table " + tableName + ": " + e.getMessage());
      }
    }
    return errors;
  }

  private static void putStreamProperty(Map<String, String> configs, String streamType, String suffix,
      String value) {
    if (value != null) {
      configs.put(StreamConfigProperties.constructStreamProperty(streamType, suffix), value);
    }
  }

  private static String orDefault(String value, String fallback) {
    return isBlank(value) ? fallback : value.trim();
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
