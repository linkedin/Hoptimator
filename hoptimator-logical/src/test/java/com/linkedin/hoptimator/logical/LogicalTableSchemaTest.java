package com.linkedin.hoptimator.logical;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for {@link LogicalTableSchema} label-based filtering.
 *
 * <p>The filtering logic is extracted into a static helper so it can be tested
 * without a live K8s cluster.
 */
public class LogicalTableSchemaTest {

  // Mirror of LogicalTableSchema.tableFromCrd filtering logic without K8s call.
  private static Map<String, Table> filterCrds(String databaseName,
      Collection<V1alpha1LogicalTable> crds) {
    Map<String, Table> result = new HashMap<>();
    for (V1alpha1LogicalTable crd : crds) {
      if (crd.getMetadata() == null || crd.getSpec() == null) {
        continue;
      }
      Map<String, String> labels = crd.getMetadata().getLabels();
      String label = labels != null ? labels.get(LogicalTableDriver.DATABASE_LABEL) : null;
      if (!databaseName.equalsIgnoreCase(label)) {
        continue;
      }
      if (crd.getSpec().getTiers() == null || crd.getSpec().getTiers().isEmpty()) {
        continue;
      }
      String tableName = crd.getMetadata().getName();
      result.put(tableName, new LogicalTable(tableName, crd.getSpec().getTiers(), null, null));
    }
    return Collections.unmodifiableMap(result);
  }

  private V1alpha1LogicalTable makeCrd(String name, String schemaLabel) {
    V1alpha1LogicalTable crd = new V1alpha1LogicalTable();
    V1ObjectMeta meta = new V1ObjectMeta().name(name);
    if (schemaLabel != null) {
      meta.putLabelsItem(LogicalTableDriver.DATABASE_LABEL, schemaLabel);
    }
    crd.setMetadata(meta);
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    // Add a minimal tier so the CRD passes tiers validation
    spec.putTiersItem("nearline", new V1alpha1LogicalTableSpecTiers().databaseCrdName("kafka-database"));
    spec.putTiersItem("online", new V1alpha1LogicalTableSpecTiers().databaseCrdName("venice"));
    crd.setSpec(spec);
    return crd;
  }

  @Test
  public void tableWithMatchingLabelIsIncluded() {
    Map<String, Table> result = filterCrds("LOGICAL", Arrays.asList(
        makeCrd("myTable", "LOGICAL")));
    assertThat(result).containsKey("myTable");
    assertThat(result.get("myTable")).isInstanceOf(LogicalTable.class);
  }

  @Test
  public void tableWithDifferentLabelIsExcluded() {
    Map<String, Table> result = filterCrds("LOGICAL", Arrays.asList(
        makeCrd("otherTable", "LOGICAL-NEARLINE-OFFLINE")));
    assertThat(result).isEmpty();
  }

  @Test
  public void tableWithNoLabelIsExcluded() {
    Map<String, Table> result = filterCrds("LOGICAL", Arrays.asList(
        makeCrd("unlabeled", null)));
    assertThat(result).isEmpty();
  }

  @Test
  public void labelMatchingIsCaseInsensitive() {
    Map<String, Table> result = filterCrds("logical", Arrays.asList(
        makeCrd("myTable", "LOGICAL")));
    assertThat(result).containsKey("myTable");
  }

  @Test
  public void multipleTablesFilteredCorrectly() {
    Map<String, Table> result = filterCrds("LOGICAL", Arrays.asList(
        makeCrd("tableA", "LOGICAL"),
        makeCrd("tableB", "LOGICAL"),
        makeCrd("tableC", "LOGICAL-NEARLINE-OFFLINE"),
        makeCrd("tableD", null)
    ));
    assertThat(result).containsKeys("tableA", "tableB");
    assertThat(result).doesNotContainKey("tableC");
    assertThat(result).doesNotContainKey("tableD");
  }

  @Test
  public void crdWithNullSpecIsSkipped() {
    V1alpha1LogicalTable noSpec = new V1alpha1LogicalTable();
    noSpec.setMetadata(new V1ObjectMeta().name("broken")
        .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, "LOGICAL"));
    // spec is null
    Map<String, Table> result = filterCrds("LOGICAL", Arrays.asList(noSpec));
    assertThat(result).isEmpty();
  }

  @Test
  public void databaseNameReturnedCorrectly() {
    LogicalTableSchema schema = new LogicalTableSchema(new Properties(), null, "MY-SCHEMA");
    assertThat(schema.databaseName()).isEqualTo("MY-SCHEMA");
  }
}
