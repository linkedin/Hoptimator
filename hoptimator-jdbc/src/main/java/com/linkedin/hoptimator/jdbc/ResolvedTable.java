package com.linkedin.hoptimator.jdbc;

import java.util.List;
import java.util.Map;

/** Metadata required to access a table in the data plane. */
public class ResolvedTable {

  private final List<String> tablePath;
  private org.apache.avro.Schema avroSchema;
  private final Map<String, String> sourceConnectorConfigs;
  private final Map<String, String> sinkConnectorConfigs;

  public ResolvedTable(List<String> tablePath, org.apache.avro.Schema avroSchema,
      Map<String, String> sourceConnectorConfigs, Map<String, String> sinkConnectorConfigs) {
    this.tablePath = tablePath;
    this.avroSchema = avroSchema;
    this.sourceConnectorConfigs = sourceConnectorConfigs;
    this.sinkConnectorConfigs = sinkConnectorConfigs;
  }

  public Map<String, String> sourceConnectorConfigs() {
    return sourceConnectorConfigs;
  }

  public Map<String, String> sinkConnectorConfigs() {
    return sinkConnectorConfigs;
  }

  public org.apache.avro.Schema avroSchema() {
    return avroSchema;
  }

  public String avroSchemaString() {
    return avroSchema.toString(true);
  }
}
