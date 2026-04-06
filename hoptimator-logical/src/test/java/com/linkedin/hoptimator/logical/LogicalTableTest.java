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
  public void emptyRowTypeWhenContextIsNull() {
    // Without a K8sContext, row type resolution returns empty struct
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), null, null);
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertThat(result.isStruct()).isTrue();
    assertThat(result.getFieldList()).isEmpty();
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
  public void tiersExposedCorrectly() {
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), null, null);
    assertThat(table.tiers()).containsKey("nearline");
    assertThat(table.tiers()).containsKey("online");
    assertThat(table.tiers().get("nearline").getDatabaseCrdName()).isEqualTo("kafka-database");
  }

  @Test
  public void resolvedDatabaseCrdNameReturnsNullWithoutTierHint() {
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), null, null);
    assertThat(table.resolvedDatabaseCrdName()).isNull();
  }

  @Test
  public void resolvedDatabaseCrdNameUsesHint() {
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), "nearline", null);
    assertThat(table.resolvedDatabaseCrdName()).isEqualTo("kafka-database");
  }

  @Test
  public void resolvedDatabaseCrdNameReturnsNullForUnknownTier() {
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), "offline", null);
    assertThat(table.resolvedDatabaseCrdName()).isNull();
  }

  @Test
  public void specConvenienceConstructorRequiresNonNullTiers() {
    // The spec convenience constructor (package-private, for testing) passes spec.getTiers()
    // which will throw if tiers are null/empty.
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    // spec.getTiers() returns null by default — expect exception
    assertThatThrownBy(() -> new LogicalTable("bad", spec))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
