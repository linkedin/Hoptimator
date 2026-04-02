package com.linkedin.hoptimator.logical;

import java.util.Collections;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import com.linkedin.hoptimator.avro.AvroConverter;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;


/**
 * A Calcite table backed by a {@code LogicalTable} CRD. The row type is derived
 * from the Avro schema stored in the CRD spec.
 */
public class LogicalTable extends AbstractTable {

  private final String name;
  private final V1alpha1LogicalTableSpec spec;

  public LogicalTable(String name, V1alpha1LogicalTableSpec spec) {
    this.name = name;
    this.spec = spec;
  }

  /** Returns the table name. */
  public String name() {
    return name;
  }

  /**
   * Returns the tier map for this logical table.
   * Keys are tier names (e.g. "nearline", "offline", "online");
   * values carry the physical database CRD binding.
   */
  public Map<String, V1alpha1LogicalTableSpecTiers> tiers() {
    if (spec.getTiers() == null) {
      return Collections.emptyMap();
    }
    return spec.getTiers();
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    String avroSchemaJson = spec.getAvroSchema();
    if (avroSchemaJson == null || avroSchemaJson.isEmpty()) {
      // Return an empty struct when no schema is available.
      return typeFactory.builder().build();
    }
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaJson);
    return AvroConverter.rel(avroSchema, typeFactory);
  }
}
