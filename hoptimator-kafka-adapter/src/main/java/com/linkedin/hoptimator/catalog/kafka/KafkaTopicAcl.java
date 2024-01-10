package com.linkedin.hoptimator.catalog.kafka;

import com.linkedin.hoptimator.catalog.Resource;

import java.util.Locale;

class KafkaTopicAcl extends Resource {
  public KafkaTopicAcl(String topicName, String principal, String method) {
    super("KafkaTopicAcl");
    export("topicName", topicName);
    export("principal", principal);
    export("method", method);
  }
}

