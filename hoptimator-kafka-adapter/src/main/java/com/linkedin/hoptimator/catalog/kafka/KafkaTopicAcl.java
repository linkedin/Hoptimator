package com.linkedin.hoptimator.catalog.kafka;

import com.linkedin.hoptimator.catalog.Resource;

public class KafkaTopicAcl extends Resource {
  public KafkaTopicAcl(String name, String principal, String method) {
    super("KafkaTopicAcl");
    export("name", name);
    export("principal", principal);
    export("method", method);
  }
}

