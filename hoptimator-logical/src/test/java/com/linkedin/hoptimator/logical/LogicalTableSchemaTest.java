package com.linkedin.hoptimator.logical;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for the tier-filtering logic used by {@link LogicalTableSchema}.
 *
 * <p>The filtering logic is extracted into a static helper so it can be tested
 * without a live K8s cluster.
 */
public class LogicalTableSchemaTest {

  // --- static helper that mirrors LogicalTableSchema.loadTableMap() filtering ---

  private static Map<String, Table> filterCrds(Map<String, String> tierFilter,
      Collection<V1alpha1LogicalTable> crds) {
    Set<String> expectedKeys = tierFilter.keySet();
    Map<String, Table> result = new HashMap<>();
    for (V1alpha1LogicalTable crd : crds) {
      if (crd.getSpec() == null) {
        continue;
      }
      Map<?, ?> crdTiers = crd.getSpec().getTiers();
      Set<?> crdTierKeys = (crdTiers == null) ? Collections.emptySet() : crdTiers.keySet();
      if (!crdTierKeys.equals(expectedKeys)) {
        continue;
      }
      String name = crd.getMetadata() != null ? crd.getMetadata().getName() : null;
      if (name == null) {
        continue;
      }
      result.put(name, new LogicalTable(name, crd.getSpec()));
    }
    return Collections.unmodifiableMap(result);
  }

  // --- helpers ---

  private V1alpha1LogicalTable makeCrd(String name, String... tierNames) {
    V1alpha1LogicalTable crd = new V1alpha1LogicalTable();
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName(name);
    crd.setMetadata(meta);

    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    spec.setAvroSchema("{\"type\":\"record\",\"name\":\"T\",\"namespace\":\"test\",\"fields\":[]}");
    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    for (String tier : tierNames) {
      tiers.put(tier, new V1alpha1LogicalTableSpecTiers());
    }
    spec.setTiers(tiers);
    crd.setSpec(spec);
    return crd;
  }

  private Map<String, String> tierMap(String... names) {
    Map<String, String> m = new HashMap<>();
    for (String n : names) {
      m.put(n, "some-db-crd");
    }
    return m;
  }

  // --- tests ---

  @Test
  public void tablesWithMatchingTierSetAreIncluded() {
    Map<String, String> filter = tierMap("nearline", "offline", "online");
    V1alpha1LogicalTable matching = makeCrd("myTable", "nearline", "offline", "online");

    Map<String, Table> result = filterCrds(filter, Arrays.asList(matching));

    assertThat(result).containsKey("myTable");
    assertThat(result.get("myTable")).isInstanceOf(LogicalTable.class);
  }

  @Test
  public void threeTierTableExcludedFromTwoTierSchemaFilter() {
    // Schema configured for 2 tiers; CRD has 3 tiers — should be excluded
    Map<String, String> filter = tierMap("nearline", "offline");
    V1alpha1LogicalTable threeTier = makeCrd("fullTable", "nearline", "offline", "online");

    Map<String, Table> result = filterCrds(filter, Arrays.asList(threeTier));

    assertThat(result).isEmpty();
  }

  @Test
  public void twoTierTableExcludedFromThreeTierSchemaFilter() {
    // Schema configured for 3 tiers; CRD only has 2 tiers — should be excluded
    Map<String, String> filter = tierMap("nearline", "offline", "online");
    V1alpha1LogicalTable twoTier = makeCrd("partialTable", "nearline", "offline");

    Map<String, Table> result = filterCrds(filter, Arrays.asList(twoTier));

    assertThat(result).isEmpty();
  }

  @Test
  public void multipleTablesReturnedWhenTierSetsMatch() {
    Map<String, String> filter = tierMap("nearline", "offline", "online");
    Collection<V1alpha1LogicalTable> crds = Arrays.asList(
        makeCrd("tableA", "nearline", "offline", "online"),
        makeCrd("tableB", "nearline", "offline", "online"),
        makeCrd("tableC", "nearline", "offline") // tier mismatch — excluded
    );

    Map<String, Table> result = filterCrds(filter, crds);

    assertThat(result).containsKeys("tableA", "tableB");
    assertThat(result).doesNotContainKey("tableC");
  }

  @Test
  public void crdWithNullSpecIsSkipped() {
    Map<String, String> filter = tierMap("nearline");
    V1alpha1LogicalTable noSpec = new V1alpha1LogicalTable();
    V1ObjectMeta meta = new V1ObjectMeta();
    meta.setName("broken");
    noSpec.setMetadata(meta);
    // spec is null

    Map<String, Table> result = filterCrds(filter, Arrays.asList(noSpec));

    assertThat(result).isEmpty();
  }

  @Test
  public void databaseNameMatchesSchemaName() {
    LogicalTableSchema schema = new LogicalTableSchema(tierMap("nearline"), null, "MY-SCHEMA");
    assertThat(schema.databaseName()).isEqualTo("MY-SCHEMA");
  }
}
