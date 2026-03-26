package com.linkedin.hoptimator.catalog.flink;

import com.linkedin.hoptimator.catalog.Resource;


public class FlinkStreamingSqlJob extends Resource {

  public FlinkStreamingSqlJob(String namespace, String name) {
    super("FlinkStreamingSqlJob");
    export("namespace", namespace);
    export("name", name);
  }
}
