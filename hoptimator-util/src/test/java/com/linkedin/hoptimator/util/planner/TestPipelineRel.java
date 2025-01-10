package com.linkedin.hoptimator.util.planner;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static com.linkedin.hoptimator.util.planner.PipelineRel.Implementor.addKeysAsOption;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPipelineRel {

  @Test
  public void testKeyOptions() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder primitiveKeyBuilder = new RelDataTypeFactory.Builder(typeFactory);
    primitiveKeyBuilder.add("KEY", SqlTypeName.VARCHAR);
    primitiveKeyBuilder.add("intField", SqlTypeName.INTEGER);
    Map<String, String> keyOptions = addKeysAsOption(new HashMap<>(), primitiveKeyBuilder.build());
    assertTrue(keyOptions.isEmpty());

    RelDataTypeFactory.Builder keyBuilder = new RelDataTypeFactory.Builder(typeFactory);
    keyBuilder.add("keyInt", SqlTypeName.INTEGER);
    keyBuilder.add("keyString", SqlTypeName.VARCHAR);
    RelDataTypeFactory.Builder recordBuilder = new RelDataTypeFactory.Builder(typeFactory);
    recordBuilder.add("intField", SqlTypeName.INTEGER);
    recordBuilder.add("KEY", keyBuilder.build());
    keyOptions = addKeysAsOption(new HashMap<>(), recordBuilder.build());
    assertEquals(3, keyOptions.size());
    assertEquals("KEY_keyInt;KEY_keyString", keyOptions.get("keys"));
    assertEquals("KEY_", keyOptions.get("keyPrefix"));
    assertEquals("RECORD", keyOptions.get("keyType"));
  }
}
