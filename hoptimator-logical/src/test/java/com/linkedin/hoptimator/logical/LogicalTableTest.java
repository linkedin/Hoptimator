package com.linkedin.hoptimator.logical;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


/**
 * Unit tests for {@link LogicalTable}.
 *
 * <p>K8s-dependent row type resolution (resolveRowType hitting real K8s) is covered
 * by the integration tests in k8s-logical.id.
 */
public class LogicalTableTest {

  private static final RelDataTypeFactory TYPE_FACTORY =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private static Map<String, V1alpha1LogicalTableSpecTiers> sampleTiers() {
    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    tiers.put("nearline", new V1alpha1LogicalTableSpecTiers().databaseCrdName("kafka-database"));
    tiers.put("online", new V1alpha1LogicalTableSpecTiers().databaseCrdName("venice"));
    return tiers;
  }

  @Test
  public void constructorRejectsNullTiers() {
    assertThatThrownBy(() -> new LogicalTable("bad", null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one tier");
  }

  @Test
  public void constructorRejectsEmptyTiers() {
    assertThatThrownBy(() -> new LogicalTable("bad", new HashMap<>(), null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one tier");
  }

  @Test
  public void specConvenienceConstructorRequiresNonNullTiers() {
    // spec.getTiers() null → constructor validation should reject
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    assertThatThrownBy(() -> new LogicalTable("bad", spec))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void resolveRowTypeThrowsOnUnknownResolvedTier() {
    // resolvedTier="offline" is not in sampleTiers() which only has nearline+online
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), "offline", null);
    assertThatThrownBy(() -> table.getRowType(TYPE_FACTORY))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("offline");
  }

  @Test
  public void unknownTypeReturnedWhenContextIsNull() {
    // null context → resolveRowType cannot fetch DB CRD → returns null → unknown type
    // This is the expected behavior for the spec convenience constructor (test use)
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), null, null);
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertThat(result).isNotNull();
    // Returns unknown type (not a struct with fields), as there's no K8s to resolve from
  }
}
