package com.linkedin.hoptimator.catalog.flink;

import com.linkedin.hoptimator.catalog.Resource;

import java.util.Map;


public class FlinkStreamingSqlJob extends Resource {

  public FlinkStreamingSqlJob(String namespace, String name, String sql) {
    super("FlinkStreamingSqlJob");
    export("namespace", namespace);
    export("name", name);
    export("sql", sql);
  }

  public FlinkStreamingSqlJob(String namespace, String name, String sql, Map<String, String> files) {
    this(namespace, name, sql);
    if (files != null && !files.isEmpty()) {
      export("files", files);
    }
  }
}
