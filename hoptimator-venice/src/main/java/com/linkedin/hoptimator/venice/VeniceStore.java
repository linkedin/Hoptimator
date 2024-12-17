package com.linkedin.hoptimator.venice;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import com.linkedin.hoptimator.avro.AvroConverter;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;


/** A batch of records from a Venice store. */
public class VeniceStore extends AbstractTable {

  private static final String KEY_PREFIX = "KEY_";
  private final StoreSchemaFetcher storeSchemaFetcher;

  public VeniceStore(StoreSchemaFetcher storeSchemaFetcher) {
    this.storeSchemaFetcher = storeSchemaFetcher;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    Schema keySchema = storeSchemaFetcher.getKeySchema();
    Schema valueSchema = storeSchemaFetcher.getLatestValueSchema();

    // Modify keySchema fields to contain a prefix of "KEY_" for their name in order to not clash with the value schema
    // Combine fields from both the modified keySchema and the valueSchema
    List<Schema.Field> combinedFields = new ArrayList<>();
    Schema combinedSchema;
    if (keySchema.getType() == Schema.Type.RECORD) {
      for (Schema.Field field : keySchema.getFields()) {
        Schema.Field modifiedField =
            new Schema.Field(KEY_PREFIX + field.name(), field.schema(), field.doc(), field.defaultVal(), field.order());
        combinedFields.add(modifiedField);
      }
    } else {
      Schema.Field modifiedField =
          new Schema.Field(KEY_PREFIX + keySchema.getName(), keySchema, keySchema.getDoc());
      combinedFields.add(modifiedField);
    }

    if (valueSchema.getType() == Schema.Type.RECORD) {
      for (Schema.Field field : valueSchema.getFields()) {
        Schema.Field copiedField =
            new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order());
        combinedFields.add(copiedField);
      }
      combinedSchema = Schema.createRecord(valueSchema.getName(), valueSchema.getDoc(), valueSchema.getNamespace(),
          keySchema.isError() || valueSchema.isError(), combinedFields);
    } else {
      Schema.Field copiedField =
          new Schema.Field(valueSchema.getName(), valueSchema, valueSchema.getDoc());
      combinedFields.add(copiedField);
      combinedSchema = Schema.createRecord("VeniceSchema", null, null, false, combinedFields);
    }
    return AvroConverter.rel(combinedSchema, typeFactory);
  }

  protected RelDataType rel(Schema schema, RelDataTypeFactory typeFactory) {
    return AvroConverter.rel(schema, typeFactory);
  }
}
