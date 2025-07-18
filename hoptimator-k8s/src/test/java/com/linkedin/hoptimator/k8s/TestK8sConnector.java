package com.linkedin.hoptimator.k8s;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import static com.linkedin.hoptimator.k8s.K8sConnector.addKeysAsOption;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TestK8sConnector {

  @Test
  public void testKeyOptions() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder primitiveKeyBuilder = new RelDataTypeFactory.Builder(typeFactory);
    primitiveKeyBuilder.add("KEY", SqlTypeName.VARCHAR);
    primitiveKeyBuilder.add("intField", SqlTypeName.INTEGER);
    Map<String, String> keyOptions = addKeysAsOption(new HashMap<>(), primitiveKeyBuilder.build());
    assertTrue(keyOptions.isEmpty());

    RelDataTypeFactory.Builder recordBuilder = new RelDataTypeFactory.Builder(typeFactory);
    recordBuilder.add("KEY_int", SqlTypeName.INTEGER);
    recordBuilder.add("KEY_string", SqlTypeName.VARCHAR);
    recordBuilder.add("intField", SqlTypeName.INTEGER);
    keyOptions = addKeysAsOption(new HashMap<>(), recordBuilder.build());
    assertEquals(3, keyOptions.size());
    assertEquals("KEY_int;KEY_string", keyOptions.get("keys"));
    assertEquals("KEY_", keyOptions.get("keyPrefix"));
    assertEquals("RECORD", keyOptions.get("keyType"));
  }
}
