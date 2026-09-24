package com.linkedin.hoptimator.pinot;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for REALTIME (Kafka) ingestion against the docker stack in
 * {@code deploy/docker/pinot} (Pinot + a single-node Kafka on the same compose network).
 *
 * <p>Exercises the actual OSS realtime code path end-to-end: it builds the table's stream config via
 * {@link PinotStreamConfigs} (exactly as {@link PinotDeployer} does), creates the REALTIME table
 * through the controller, produces a few JSON records to the topic, and polls the broker until the
 * Pinot server has consumed them — proving the synthesized stream config actually ingests.
 *
 * <p>Two Kafka addresses are in play: the Pinot server consumes via the compose-internal listener
 * ({@code kafka:9092}), while this host-side test produces via the external listener
 * ({@code localhost:9093}). Override with {@code -Dpinot.kafka.brokerList=...} /
 * {@code -Dkafka.bootstrap=...} / {@code -Dpinot.controllerUrl=...} / {@code -Dpinot.brokerUrl=...}.
 */
@Tag("integration")
public class PinotRealtimeIntegrationTest {

  private static final String CONTROLLER_URL =
      System.getProperty("pinot.controllerUrl", "http://localhost:9000");
  private static final String BROKER_QUERY_URL =
      System.getProperty("pinot.brokerUrl", "http://localhost:8000") + "/query/sql";
  // What the host-side producer/admin use (external listener).
  private static final String HOST_BOOTSTRAP = System.getProperty("kafka.bootstrap", "localhost:9093");
  // What the Pinot server uses to consume (compose-internal listener).
  private static final String PINOT_BROKER_LIST = System.getProperty("pinot.kafka.brokerList", "kafka:9092");
  private static final String TABLE = "hoptimator_it_realtime";
  private static final int RECORD_COUNT = 3;
  private static final Duration CONSUME_TIMEOUT = Duration.ofSeconds(120);

  private final HttpClient httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  @Test
  void realtimeIngestionConsumesFromKafka() throws Exception {
    createTopic();
    try (PinotControllerClient client = new PinotControllerClient(CONTROLLER_URL)) {
      client.addSchema(schema(), true);
      addRealtimeTableWhenReady(client, realtimeTableConfig());
      try {
        assertThat(client.tableExists(TABLE)).isTrue();
        produceRecords();
        long consumed = pollForRowCount();
        assertThat(consumed).isGreaterThanOrEqualTo(RECORD_COUNT);
      } finally {
        client.dropTable(TABLE);
        client.dropSchema(TABLE);
      }
    } finally {
      deleteTopic();
    }
  }

  private void createTopic() throws Exception {
    try (Admin admin = Admin.create(adminProps())) {
      admin.createTopics(Collections.singletonList(new NewTopic(TABLE, 1, (short) 1))).all().get();
    }
  }

  /**
   * Creates the REALTIME table, tolerating the brief window after the cluster reports healthy (the
   * controller port is open) but before the server has finished joining and been tagged for the
   * REALTIME tenant — during which the controller rejects the table with a 400 "No instance found
   * with tag: DefaultTenant_REALTIME".
   */
  private void addRealtimeTableWhenReady(PinotControllerClient client, TableConfig config) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
    while (true) {
      try {
        client.addTable(config);
        return;
      } catch (IOException e) {
        String message = e.getMessage();
        boolean tenantNotReady = message != null && message.contains("instance");
        if (tenantNotReady && System.nanoTime() < deadline) {
          Thread.sleep(3000);
          continue;
        }
        throw e;
      }
    }
  }

  private void deleteTopic() {
    try (Admin admin = Admin.create(adminProps())) {
      admin.deleteTopics(Collections.singletonList(TABLE)).all().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException | RuntimeException e) {
      // Best-effort cleanup; don't fail the test on teardown.
    }
  }

  private void produceRecords() throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", HOST_BOOTSTRAP);
    try (KafkaProducer<String, String> producer =
        new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
      long now = System.currentTimeMillis();
      for (int i = 0; i < RECORD_COUNT; i++) {
        String value = "{\"id\":\"row-" + i + "\",\"ts\":" + (now + i) + "}";
        producer.send(new ProducerRecord<>(TABLE, "row-" + i, value)).get();
      }
      producer.flush();
    }
  }

  /** Polls the broker's {@code COUNT(*)} until the server has consumed the produced rows (or times out). */
  private long pollForRowCount() throws Exception {
    long deadline = System.nanoTime() + CONSUME_TIMEOUT.toNanos();
    long count = 0;
    while (System.nanoTime() < deadline) {
      count = queryRowCount();
      if (count >= RECORD_COUNT) {
        return count;
      }
      Thread.sleep(2000);
    }
    return count;
  }

  private long queryRowCount() throws Exception {
    String body = "{\"sql\":\"SELECT COUNT(*) FROM " + TABLE + "\"}";
    HttpRequest request = HttpRequest.newBuilder(URI.create(BROKER_QUERY_URL))
        .header("Content-Type", "application/json")
        .timeout(Duration.ofSeconds(15))
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2) {
      return 0;
    }
    var root = JsonUtils.stringToJsonNode(response.body());
    var resultTable = root.get("resultTable");
    if (resultTable == null || !resultTable.has("rows") || resultTable.get("rows").isEmpty()) {
      return 0;
    }
    return resultTable.get("rows").get(0).get(0).asLong();
  }

  private static Properties adminProps() {
    Properties props = new Properties();
    props.put("bootstrap.servers", HOST_BOOTSTRAP);
    return props;
  }

  private static Schema schema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(TABLE)
        .addDimensionField("id", FieldSpec.DataType.STRING)
        .addDateTimeField("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  /** Builds the REALTIME table config exactly as {@link PinotDeployer} does, via {@link PinotStreamConfigs}. */
  private static TableConfig realtimeTableConfig() {
    Map<String, String> hints = Map.of(
        PinotStreamConfigs.KAFKA_TOPIC, TABLE,
        PinotStreamConfigs.KAFKA_BROKER_LIST, PINOT_BROKER_LIST);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(Collections.singletonList(PinotStreamConfigs.build(hints))));
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE)
        .setTimeColumnName("ts")
        .setNumReplicas(1)
        .setIngestionConfig(ingestionConfig)
        .build();
  }
}
