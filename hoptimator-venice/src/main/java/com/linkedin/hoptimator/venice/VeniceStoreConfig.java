package com.linkedin.hoptimator.venice;

import java.util.Map;

public class VeniceStoreConfig {
    public static final String KEY_VALUE_SCHEMA_ID = "valueSchemaId";
    private final Map<String, String> storeConfigs;

    public VeniceStoreConfig(Map<String, String> storeConfigs) {
        this.storeConfigs = storeConfigs;
    }

    public Integer getValueSchemaId() {
        if (storeConfigs != null) {
            String schemaIdStr = storeConfigs.get(KEY_VALUE_SCHEMA_ID);
            if (schemaIdStr != null) {
                try {
                    return Integer.parseInt(schemaIdStr);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid valueSchemaId: " + schemaIdStr, e);
                }
            }
        }
        return null;
    }
}
