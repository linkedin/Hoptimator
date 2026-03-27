package com.linkedin.hoptimator.venice;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


class VeniceStoreConfigTest {

  @Test
  void testGetValueSchemaIdReturnsIntegerWhenPresent() {
    Map<String, String> config = Map.of(VeniceStoreConfig.KEY_VALUE_SCHEMA_ID, "42");
    VeniceStoreConfig storeConfig = new VeniceStoreConfig(config);
    assertEquals(42, storeConfig.getValueSchemaId());
  }

  @Test
  void testGetValueSchemaIdReturnsNullWhenMissing() {
    Map<String, String> config = Collections.emptyMap();
    VeniceStoreConfig storeConfig = new VeniceStoreConfig(config);
    assertNull(storeConfig.getValueSchemaId());
  }

  @Test
  void testGetValueSchemaIdReturnsNullWhenConfigIsNull() {
    VeniceStoreConfig storeConfig = new VeniceStoreConfig(null);
    assertNull(storeConfig.getValueSchemaId());
  }

  @Test
  void testGetValueSchemaIdThrowsForInvalidNumber() {
    Map<String, String> config = Map.of(VeniceStoreConfig.KEY_VALUE_SCHEMA_ID, "not-a-number");
    VeniceStoreConfig storeConfig = new VeniceStoreConfig(config);
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, storeConfig::getValueSchemaId);
    assertEquals("Invalid valueSchemaId: not-a-number", ex.getMessage());
  }

  @Test
  void testKeyValueSchemaIdConstant() {
    assertEquals("valueSchemaId", VeniceStoreConfig.KEY_VALUE_SCHEMA_ID);
  }
}
