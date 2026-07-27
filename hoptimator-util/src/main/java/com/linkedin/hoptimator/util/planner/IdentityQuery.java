package com.linkedin.hoptimator.util.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;


/**
 * Builds an identity query — {@code SELECT * FROM <path>} — as a {@link RelNode}, <em>without</em> a
 * Calcite {@code Connection} or catalog. This is for pipelines whose source and sink share a row
 * type and therefore need no real query planning, e.g. a logical table's inter-tier copies: because
 * both tier tables were created from the same row type, moving data between them is a pure identity
 * projection. The resulting {@code RelNode} renders (via {@code RelToSqlConverter}) to the same
 * {@code SELECT * FROM catalog.schema.table} the SQL planner would have produced.
 */
public final class IdentityQuery {

  private IdentityQuery() {
  }

  /** An identity {@code TableScan} over {@code path} exposing {@code rowType}. */
  public static RelNode scan(List<String> path, RelDataType rowType) {
    SchemaPlus root = Frameworks.createRootSchema(false);
    SchemaPlus parent = root;
    for (String part : path.subList(0, path.size() - 1)) {
      parent = parent.add(part, new AbstractSchema());
    }
    parent.add(path.get(path.size() - 1), new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory factory) {
        // Re-home the row type into the builder's type factory to avoid cross-factory issues.
        RelDataTypeFactory.Builder builder = factory.builder();
        for (RelDataTypeField field : rowType.getFieldList()) {
          builder.add(field.getName(), field.getType());
        }
        return builder.build();
      }
    });
    FrameworkConfig config = Frameworks.newConfigBuilder().defaultSchema(root).build();
    return RelBuilder.create(config).scan(path).build();
  }

  /** The identity target-field list {@code [(0, f0), (1, f1), ...]} for {@code rowType}. */
  public static ImmutablePairList<Integer, String> fields(RelDataType rowType) {
    PairList<Integer, String> fields = PairList.of();
    int index = 0;
    for (RelDataTypeField field : rowType.getFieldList()) {
      fields.add(index++, field.getName());
    }
    return fields.immutable();
  }
}
