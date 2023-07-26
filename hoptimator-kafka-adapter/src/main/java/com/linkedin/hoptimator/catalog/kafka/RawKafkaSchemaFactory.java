package com.linkedin.hoptimator.catalog.kafka;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.kafka.clients.admin.AdminClient;

import com.linkedin.hoptimator.catalog.ConfigProvider;
import com.linkedin.hoptimator.catalog.DataType;
import com.linkedin.hoptimator.catalog.Database;
import com.linkedin.hoptimator.catalog.DatabaseSchema;
import com.linkedin.hoptimator.catalog.ResourceProvider;
import com.linkedin.hoptimator.catalog.TableLister;
import com.linkedin.hoptimator.catalog.TableResolver;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RawKafkaSchemaFactory implements SchemaFactory {

  @Override
  @SuppressWarnings("unchecked")
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    String principal = (String) operand.getOrDefault("principal", "User:ANONYMOUS");
    Map<String, Object> clientConfig = (Map<String, Object>) operand.get("clientConfig");
    DataType.Struct rowType = DataType.struct()
      .with("PAYLOAD", DataType.VARCHAR_NULL)
      .with("KEY", DataType.VARCHAR_NULL);
    ConfigProvider connectorConfigProvider = ConfigProvider.from(clientConfig)
      .withPrefix("properties.")
      .with("connector", "upsert-kafka")
      .with("key.format", "csv")
      .with("value.format", "csv")
      .with("value.fields-include", "EXCEPT_KEY")
      .with("topic", x -> x);
    TableLister tableLister = () -> {
      AdminClient client = AdminClient.create(clientConfig);
      Collection<String> topics = client.listTopics().names().get();
      client.close();
      return topics;
    };
    ConfigProvider topicConfigProvider = ConfigProvider.from(clientConfig);
    TableResolver resolver = x -> rowType.rel();
    
    ResourceProvider resources = ResourceProvider.empty()
      .with(x -> new KafkaTopic(x, topicConfigProvider.config(x)))
      .readWith(x -> new KafkaTopicAcl(x, principal, "Read"))
      .writeWith(x -> new KafkaTopicAcl(x, principal, "Write"));
    
    Database database = new Database(name, tableLister, resolver, connectorConfigProvider,
      resources);
    return new DatabaseSchema(database);
  }
}
