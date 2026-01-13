package com.linkedin.hoptimator.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.util.LazyReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.hoptimator.jdbc.schema.LazyTableLookup;


/**
 * Schema for Kafka topics with lazy loading.
 * Tables are loaded on-demand when first accessed, not during driver connection.
 */
public class ClusterSchema extends AbstractSchema {

  private static final Logger log = LoggerFactory.getLogger(ClusterSchema.class);

  private final Properties properties;
  private final LazyReference<Lookup<Table>> tables = new LazyReference<>();

  public ClusterSchema(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Lookup<Table> tables() {
    return tables.getOrCompute(() -> new LazyTableLookup<>() {

      @Override
      protected Map<String, Table> loadAllTables() throws Exception {
        try (AdminClient adminClient = AdminClient.create(properties)) {
          Set<String> topicNames = adminClient.listTopics().names().get();
          Map<String, Table> tables = new HashMap<>();
          for (String topicName : topicNames) {
            tables.put(topicName, new KafkaTopic(topicName, properties));
          }
          return tables;
        }
      }

      @Override
      protected @Nullable Table loadTable(String name) throws Exception {
        try (AdminClient adminClient = AdminClient.create(properties)) {
          // Attempt to get the topic description, which will throw an exception if it doesn't exist
          adminClient.describeTopics(Collections.singleton(name)).topicNameValues().get(name).get();
          return new KafkaTopic(name, properties);
        } catch (ExecutionException e) {
          // Check the underlying cause of the exception
          if (e.getCause() instanceof UnknownTopicOrPartitionException) {
            return null;
          }
          throw e;
        }
      }

      @Override
      protected String getSchemaDescription() {
        return "Kafka at " + properties.getProperty("bootstrap.servers");
      }
    });
  }
}
