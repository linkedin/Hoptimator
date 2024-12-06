package com.linkedin.hoptimator.kafka;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Properties;

/** A batch of records from a Kafka topic. */
public class KafkaTopic extends AbstractTable {

  private final String topicName;
  private final Properties properties;

  public KafkaTopic(String topicName, Properties properties) {
    this.topicName = topicName;
    this.properties = properties;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    return builder
        .add("KEY", SqlTypeName.VARCHAR).nullable(true)
        .add("VALUE", SqlTypeName.BINARY).nullable(true)
        .build();
  }
}
