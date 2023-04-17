package com.linkedin.hoptimator.operator.subscription;

import com.linkedin.hoptimator.catalog.Resource;

class PipelineEnvironment extends Resource.SimpleEnvironment {

  PipelineEnvironment(String namespace, String name) {
    export("pipeline.namespace", namespace);
    export("pipeline.name", name);
  }
}
