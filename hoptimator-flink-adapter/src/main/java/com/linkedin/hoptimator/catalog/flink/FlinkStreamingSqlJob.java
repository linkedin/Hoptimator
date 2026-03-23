package com.linkedin.hoptimator.catalog.flink;

import com.linkedin.hoptimator.catalog.Resource;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;


public class FlinkStreamingSqlJob extends Resource {

  public FlinkStreamingSqlJob(String namespace, String name, String sql) {
    super("FlinkStreamingSqlJob");
    export("namespace", namespace);
    export("name", name);
    export("sql", sql);
  }

  public FlinkStreamingSqlJob(String namespace, String name, String sql, Map<String, String> files) {
    this(namespace, name, prependFiles(sql, files));
  }

  /** Prepend --file directives to the SQL script so FlinkRunner can extract and write them. */
  private static String prependFiles(String sql, Map<String, String> files) {
    if (files == null || files.isEmpty()) {
      return sql;
    }
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : files.entrySet()) {
      String encoded = Base64.getEncoder().encodeToString(
          entry.getValue().getBytes(StandardCharsets.UTF_8));
      sb.append("--file:").append(entry.getKey()).append(":").append(encoded);
      sb.append(";\n");
    }
    sb.append(sql);
    return sb.toString();
  }
}
