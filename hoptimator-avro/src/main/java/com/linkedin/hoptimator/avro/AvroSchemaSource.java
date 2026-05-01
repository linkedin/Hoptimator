package com.linkedin.hoptimator.avro;

import org.apache.avro.Schema;


/**
 * Implemented by Calcite {@link org.apache.calcite.schema.Table}s backed by native Avro metadata.
 * Exposes source-of-truth key and value schemas to downstream consumers (connector deployers,
 * resolvers) without round-tripping through Calcite's {@code RelDataType} — preserving namespaces,
 * nested record names, reused record definitions, default values, and enum/fixed metadata that
 * the type system flattens away.
 *
 * <p>Consumers pick the view that matches their need:
 * <ul>
 *   <li>{@link #valueSchema()} — the record's data payload.
 *   <li>{@link #keySchema()} — the record's key schema, or {@code null} when the table has no
 *       distinct key concept. A struct key exposes its fields directly; a primitive key returns
 *       a primitive {@link Schema}.
 * </ul>
 */
public interface AvroSchemaSource {

  /**
   * Returns the value/payload Avro schema — the data record's schema without any query-layer
   * scaffolding like {@code KEY_}-prefixed key fields. Connector payload options should render
   * this.
   */
  Schema valueSchema();

  /**
   * Returns the key Avro schema, or {@code null} when this table has no distinct key concept.
   * Struct keys expose their fields directly; primitive keys return a primitive {@link Schema}.
   * SQL/query-layer consumers may merge this with {@link #valueSchema()} via
   * {@link AvroSchemas#mergedAvroSchemaFor(AvroSchemaSource)}.
   */
  default Schema keySchema() {
    return null;
  }
}
