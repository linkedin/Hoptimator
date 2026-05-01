package com.linkedin.hoptimator.avro;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;


/**
 * Utilities for composing Avro schemas. The primary use case is Hoptimator-style stores that
 * back each logical table with a separate key and value Avro schema and need to expose a single
 * flattened record (key fields prefixed, followed by value fields) without round-tripping
 * through Calcite's {@code RelDataType}.
 */
public final class AvroSchemas {

  /**
   * Standard Hoptimator prefix applied to fields contributed by a record-typed key when merging
   * into the value schema for SQL/query consumption.
   */
  public static final String KEY_PREFIX = "KEY_";

  /**
   * Standard Hoptimator field name used when a primitive key is merged into the value schema for
   * SQL/query consumption.
   */
  public static final String PRIMITIVE_KEY_NAME = "KEY";

  private AvroSchemas() {
  }

  /**
   * Produces a fresh {@link Schema.Field} clone. Avro's {@code Schema.setFields} rejects Fields
   * that already belong to another record (it tracks the field's position as a mutable guard), so
   * fields from an input schema cannot be handed to a new record directly. Cloning also lets
   * callers rename fields (e.g., apply a {@code KEY_} prefix).
   *
   * <p>Copies every attribute Avro exposes on a Field: name (from the caller), schema reference
   * (the type Schema is shared by reference — nested records, enums, namespaces, etc. survive
   * intact), doc, default value, sort order, aliases, and custom properties.
   */
  public static Schema.Field cloneField(String name, Schema.Field original) {
    Object defaultVal = original.hasDefaultValue() ? original.defaultVal() : null;
    Schema.Field clone = new Schema.Field(name, original.schema(), original.doc(), defaultVal,
        original.order());
    original.aliases().forEach(clone::addAlias);
    original.getObjectProps().forEach(clone::addProp);
    return clone;
  }

  /**
   * Produces the merged Avro schema for an {@link AvroSchemaSource}: the value schema with key
   * fields prepended using Hoptimator's standard convention ({@link #KEY_PREFIX} for struct keys,
   * {@link #PRIMITIVE_KEY_NAME} field for primitive keys). When the source has no key
   * ({@link AvroSchemaSource#keySchema()} returns {@code null}), returns the value schema
   * unchanged.
   *
   * <p>Centralizes the Hoptimator merging convention so SQL/query-layer consumers (like
   * {@code HoptimatorConnection.resolve()}) share one implementation.
   */
  public static Schema mergedAvroSchemaFor(AvroSchemaSource source) {
    Schema key = source.keySchema();
    Schema value = source.valueSchema();
    if (key == null) {
      return value;
    }
    return mergeKeyIntoValue(key, value, KEY_PREFIX, PRIMITIVE_KEY_NAME);
  }

  /**
   * Merges a key Avro schema and a value record schema into a single record that inherits the
   * value schema's identity (namespace, name, doc, isError flag), aliases, and record-level
   * custom properties. Struct keys contribute one field per key field, each prefixed with
   * {@code keyPrefix}. Primitive keys (or any non-record key) contribute a single field named
   * {@code primitiveKeyName} with the key schema as its type.
   *
   * @param keySchema         key Avro schema. If a record, its fields are prefixed and prepended.
   *                          Otherwise, a single primitive key field is prepended.
   * @param valueSchema       must be a {@link Schema.Type#RECORD}.
   * @param keyPrefix         prefix applied to each key field name when key is a record (e.g.
   *                          {@code "KEY_"}).
   * @param primitiveKeyName  field name used when key is primitive (e.g. {@code "KEY"}).
   */
  static Schema mergeKeyIntoValue(Schema keySchema, Schema valueSchema,
      String keyPrefix, String primitiveKeyName) {
    if (valueSchema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException(
          "Value schema must be a record; got " + valueSchema.getType());
    }
    List<Schema.Field> allFields = new ArrayList<>();
    if (keySchema.getType() == Schema.Type.RECORD) {
      for (Schema.Field kf : keySchema.getFields()) {
        allFields.add(cloneField(keyPrefix + kf.name(), kf));
      }
    } else {
      allFields.add(new Schema.Field(primitiveKeyName, keySchema, "Primitive key field.", null));
    }
    for (Schema.Field vf : valueSchema.getFields()) {
      allFields.add(cloneField(vf.name(), vf));
    }
    Schema merged = Schema.createRecord(
        valueSchema.getName(),
        valueSchema.getDoc(),
        valueSchema.getNamespace(),
        valueSchema.isError());
    merged.setFields(allFields);
    valueSchema.getAliases().forEach(merged::addAlias);
    valueSchema.getObjectProps().forEach(merged::addProp);
    return merged;
  }
}
