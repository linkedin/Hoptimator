package com.linkedin.hoptimator.catalog.kafka;

import com.linkedin.hoptimator.catalog.Resource;

import java.util.Map;

class KafkaTopic extends Resource {
  KafkaTopic(String topicName, Map<String, String> clientOverrides) {
    super("KafkaTopic");
    export("topicName", topicName);
    export("clientOverrides", clientOverrides);
  }
}

