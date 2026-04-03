package com.linkedin.hoptimator.logical;

import java.util.Collections;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;


/**
 * A Calcite table backed by a {@code LogicalTable} CRD.
 *
 * <p>The row type is resolved dynamically from the physical tier at schema load time
 * (by {@link LogicalTableSchema}) and passed in at construction. This ensures the
 * schema always reflects the current state of the underlying system rather than a
 * cached Avro snapshot.
 */
public class LogicalTable extends AbstractTable {

  private final String name;
  private final RelDataType rowType;
  private final Map<String, V1alpha1LogicalTableSpecTiers> tiers;

  public LogicalTable(String name, RelDataType rowType,
      Map<String, V1alpha1LogicalTableSpecTiers> tiers) {
    this.name = name;
    this.rowType = rowType;
    this.tiers = tiers != null ? tiers : Collections.emptyMap();
  }

  /** Convenience constructor from a CRD spec with an empty row type fallback. */
  public LogicalTable(String name, V1alpha1LogicalTableSpec spec) {
    this(name, null, spec.getTiers());
  }

  public String name() {
    return name;
  }

  /**
   * Returns the tier map for this logical table.
   * Keys are tier names (e.g. "nearline", "offline", "online");
   * values carry the physical database CRD binding.
   */
  public Map<String, V1alpha1LogicalTableSpecTiers> tiers() {
    return tiers;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (rowType != null) {
      return rowType;
    }
    // Row type not resolved — return empty struct.
    // This happens when the physical tier was unreachable at schema load time.
    return typeFactory.builder().build();
  }
}
