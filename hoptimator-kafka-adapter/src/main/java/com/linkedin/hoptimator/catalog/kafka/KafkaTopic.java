package com.linkedin.hoptimator.catalog.kafka;

import com.linkedin.hoptimator.catalog.Resource;

import java.util.Optional;
import java.util.Collections;
import java.util.Map;

public class KafkaTopic extends Resource {
  public KafkaTopic(String name, Integer numPartitions, Map<String, String> clientOverrides) {
    super("KafkaTopic");
    export("name", name);
    export("numPartitions", Optional.ofNullable(numPartitions)
      .map(x -> Integer.toString(x)).orElse("null"));
    export("clientOverrides", clientOverrides);
  }
}

