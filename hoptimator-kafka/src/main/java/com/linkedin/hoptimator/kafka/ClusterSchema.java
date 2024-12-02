package com.linkedin.hoptimator.kafka;

import org.apache.kafka.clients.admin.AdminClient;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ClusterSchema extends AbstractSchema {

  private static Logger log = LoggerFactory.getLogger(ClusterSchema.class);

  private final Properties properties;
  private final Map<String, Table> tableMap = new HashMap<>();

  public ClusterSchema(Properties properties) {
    this.properties = properties;
  }

  public void populate() throws InterruptedException, ExecutionException {
    tableMap.clear();
    AdminClient adminClient = AdminClient.create(properties);
    log.info("Loading Kafka topics from {} ...", properties.getProperty("bootstrap.servers"));
    Set<String> topicNames = adminClient.listTopics().names().get();
    log.info("Loaded {} topics.",  topicNames.size());
    for (String name : topicNames) {
      tableMap.put(name, new KafkaTopic(name, properties));
    }
  }

  @Override
  public Map<String, Table> getTableMap() {
    return tableMap;
  }
}
