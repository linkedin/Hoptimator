package com.linkedin.hoptimator.logical;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for {@link LogicalTable}.
 */
public class LogicalTableTest {

  private static final RelDataTypeFactory TYPE_FACTORY =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  @Test
  public void rowTypeReturnedDirectlyWhenProvided() {
    RelDataType rowType = TYPE_FACTORY.builder()
        .add("memberId", SqlTypeName.BIGINT)
        .add("pageKey", SqlTypeName.VARCHAR)
        .build();

    LogicalTable table = new LogicalTable("myTable", rowType, Collections.emptyMap());
    RelDataType result = table.getRowType(TYPE_FACTORY);

    assertThat(result.isStruct()).isTrue();
    assertThat(result.getFieldNames()).containsExactly("memberId", "pageKey");
    assertThat(Objects.requireNonNull(result.getField("memberId", false, false))
        .getType().getSqlTypeName()).isEqualTo(SqlTypeName.BIGINT);
  }

  @Test
  public void emptyRowTypeWhenNullProvided() {
    LogicalTable table = new LogicalTable("myTable", null, Collections.emptyMap());
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertThat(result.isStruct()).isTrue();
    assertThat(result.getFieldList()).isEmpty();
  }

  @Test
  public void specConvenienceConstructorHasNullRowType() {
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    LogicalTable table = new LogicalTable("myTable", spec);
    // Row type is null — getRowType returns empty struct
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertThat(result.isStruct()).isTrue();
    assertThat(result.getFieldList()).isEmpty();
  }

  @Test
  public void tiersExposedCorrectly() {
    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    tiers.put("nearline", new V1alpha1LogicalTableSpecTiers().databaseCrdName("xinfra-tracking"));
    tiers.put("online", new V1alpha1LogicalTableSpecTiers().databaseCrdName("venice"));

    LogicalTable table = new LogicalTable("myTable", null, tiers);
    assertThat(table.tiers()).containsKey("nearline");
    assertThat(table.tiers()).containsKey("online");
    assertThat(table.tiers().get("nearline").getDatabaseCrdName()).isEqualTo("xinfra-tracking");
  }
}
