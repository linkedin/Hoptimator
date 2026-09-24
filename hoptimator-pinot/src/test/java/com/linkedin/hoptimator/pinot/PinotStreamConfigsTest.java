package com.linkedin.hoptimator.pinot;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PinotStreamConfigsTest {

  private static Map<String, String> validHints() {
    Map<String, String> hints = new HashMap<>();
    hints.put("kafkaTopic", "events");
    hints.put("kafkaBrokerList", "broker1:9092");
    return hints;
  }

  @Test
  void buildAppliesDefaults() {
    Map<String, String> configs = PinotStreamConfigs.build(validHints());
    assertThat(configs).containsEntry("streamType", "kafka");
    assertThat(configs).containsEntry("stream.kafka.topic.name", "events");
    assertThat(configs).containsEntry("stream.kafka.broker.list", "broker1:9092");
    assertThat(configs).containsEntry("stream.kafka.consumer.type", "lowlevel");
    assertThat(configs).containsEntry("stream.kafka.decoder.class.name",
        PinotStreamConfigs.DEFAULT_DECODER_CLASS);
    assertThat(configs).containsEntry("stream.kafka.consumer.prop.auto.offset.reset", "smallest");
    assertThat(configs).containsKey("stream.kafka.consumer.factory.class.name");
    assertThat(configs).doesNotContainKey("realtime.segment.flush.threshold.rows");
  }

  @Test
  void buildHonorsOverridesAndFlushThreshold() {
    Map<String, String> hints = validHints();
    hints.put("streamDecoderClass", "com.example.MyDecoder");
    hints.put("streamConsumerType", "highlevel");
    hints.put("streamOffsetCriteria", "largest");
    hints.put("flushThresholdRows", "100000");

    Map<String, String> configs = PinotStreamConfigs.build(hints);
    assertThat(configs).containsEntry("stream.kafka.decoder.class.name", "com.example.MyDecoder");
    assertThat(configs).containsEntry("stream.kafka.consumer.type", "highlevel");
    assertThat(configs).containsEntry("stream.kafka.consumer.prop.auto.offset.reset", "largest");
    assertThat(configs).containsEntry("realtime.segment.flush.threshold.rows", "100000");
  }

  @Test
  void buildRespectsCustomStreamType() {
    Map<String, String> hints = validHints();
    hints.put("streamType", "kinesis");
    Map<String, String> configs = PinotStreamConfigs.build(hints);
    assertThat(configs).containsEntry("streamType", "kinesis");
    assertThat(configs).containsEntry("stream.kinesis.topic.name", "events");
  }

  @Test
  void validationPassesForWellFormedSpec() {
    assertThat(PinotStreamConfigs.validationErrors("myTable", validHints())).isEmpty();
  }

  @Test
  void validationRejectsMissingTopic() {
    Map<String, String> hints = validHints();
    hints.remove("kafkaTopic");
    assertThat(PinotStreamConfigs.validationErrors("myTable", hints)).isNotEmpty();
  }

  @Test
  void validationRejectsMissingBrokerList() {
    Map<String, String> hints = validHints();
    hints.remove("kafkaBrokerList");
    assertThat(PinotStreamConfigs.validationErrors("myTable", hints)).isNotEmpty();
  }
}
