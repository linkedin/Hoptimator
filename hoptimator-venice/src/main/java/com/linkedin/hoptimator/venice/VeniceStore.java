package com.linkedin.hoptimator.venice;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import com.linkedin.hoptimator.avro.AvroConverter;
import com.linkedin.hoptimator.util.DataTypeUtils;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;


/** A batch of records from a Venice store. */
public class VeniceStore extends AbstractTable {

  private final StoreSchemaFetcher storeSchemaFetcher;

  public VeniceStore(StoreSchemaFetcher storeSchemaFetcher) {
    this.storeSchemaFetcher = storeSchemaFetcher;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    Schema keySchema = storeSchemaFetcher.getKeySchema();
    Schema valueSchema = storeSchemaFetcher.getLatestValueSchema();

    // Venice contains both a key schema and a value schema. Since we need to pass back one joint schema,
    // and to avoid name collisions, all key fields are structured as "KEY$foo".
    RelDataType key = rel(keySchema, typeFactory);
    RelDataType value = rel(valueSchema, typeFactory);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.addAll(value.getFieldList());
    builder.add("KEY", key);
    RelDataType combinedSchema = builder.build();
    return DataTypeUtils.flatten(combinedSchema, typeFactory);
  }

  protected RelDataType rel(Schema schema, RelDataTypeFactory typeFactory) {
    return AvroConverter.rel(schema, typeFactory);
  }
}
