package com.linkedin.hoptimator.catalog.kafka;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.kafka.clients.admin.AdminClient;

import com.linkedin.hoptimator.catalog.ConfigProvider;
import com.linkedin.hoptimator.catalog.DataType;
import com.linkedin.hoptimator.catalog.Database;
import com.linkedin.hoptimator.catalog.DatabaseSchema;
import com.linkedin.hoptimator.catalog.TableLister;
import com.linkedin.hoptimator.catalog.TableResolver;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RawKafkaSchemaFactory implements SchemaFactory {

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    DataType.Struct rowType = DataType.struct()
      .with("PAYLOAD", DataType.VARCHAR_NULL);
    ConfigProvider configProvider = ConfigProvider.from(operand).withPrefix("properties.")
      .with("connector", "kafka")
      .with("format", "raw")
      .with("topic", x -> x);
    TableLister tableLister = () -> {
      AdminClient client = AdminClient.create(operand);
      Collection<String> topics = client.listTopics().names().get();
      client.close();
      return topics;
    };
    TableResolver resolver = x -> rowType.rel();
    Database database = new Database(name, tableLister, resolver, configProvider);
    return new DatabaseSchema(database);
  }
}
