package com.linkedin.hoptimator.catalog;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Enables loading an Adapter via a jdbc connection string and/or Calcite model
 * file.
 */
public final class AdapterSchemaFactory implements SchemaFactory {
  /** Public singleton, per factory contract. */
  public final static AdapterSchemaFactory INSTANCE = new AdapterSchemaFactory();

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    String adapterName = (String) operand.getOrDefault("adapter", name);
    String database = (String) operand.getOrDefault("database", name);
    return new AdapterSchema(database, AdapterService.adapter(adapterName));
  }
}
