package com.linkedin.hoptimator.kafka;

import java.util.Properties;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


class KafkaTopicTest {

  @Test
  void testGetRowTypeReturnsTwoColumns() {
    Properties props = new Properties();
    KafkaTopic topic = new KafkaTopic("test-topic", props);
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    RelDataType rowType = topic.getRowType(typeFactory);

    assertEquals(2, rowType.getFieldCount());
  }

  @Test
  void testGetRowTypeHasKeyColumn() {
    Properties props = new Properties();
    KafkaTopic topic = new KafkaTopic("test-topic", props);
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    RelDataType rowType = topic.getRowType(typeFactory);
    RelDataTypeField keyField = rowType.getField("KEY", false, false);

    assertNotNull(keyField);
    assertEquals(SqlTypeName.VARCHAR, keyField.getType().getSqlTypeName());
    assertTrue(keyField.getType().isNullable());
  }

  @Test
  void testGetRowTypeHasValueColumn() {
    Properties props = new Properties();
    KafkaTopic topic = new KafkaTopic("test-topic", props);
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    RelDataType rowType = topic.getRowType(typeFactory);
    RelDataTypeField valueField = rowType.getField("VALUE", false, false);

    assertNotNull(valueField);
    assertEquals(SqlTypeName.BINARY, valueField.getType().getSqlTypeName());
    assertTrue(valueField.getType().isNullable());
  }
}
