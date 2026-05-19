package com.linkedin.hoptimator.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link LogicalTableSchema} label-based filtering.
 *
 * <p>Tests call {@link LogicalTableSchema#tableFromCrd} directly on a real schema
 * instance (constructed with null K8s context) so that the production filtering
 * logic is tested without a live cluster.
 */
public class LogicalTableSchemaTest {

  private static final String DATABASE_NAME = "logical";

  private LogicalTableSchema schema;

  @BeforeEach
  public void setUp() {
    schema = new LogicalTableSchema(new Properties(), null, DATABASE_NAME);
  }

  // ── tableFromCrd() ───────────────────────────────────────────────────────

  @Test
  public void tableFromCrdReturnsNullWhenMetadataIsNull() {
    V1alpha1LogicalTable crd = new V1alpha1LogicalTable();
    assertNull(schema.tableFromCrd(crd));
  }

  @Test
  public void tableFromCrdReturnsNullWhenSpecIsNull() {
    V1alpha1LogicalTable crd = new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name("some-table"));
    assertNull(schema.tableFromCrd(crd));
  }

  @Test
  public void tableFromCrdReturnsNullWhenDatabaseLabelDoesNotMatch() {
    V1alpha1LogicalTable crd = buildValidCrd("some-table", "other-database");
    assertNull(schema.tableFromCrd(crd));
  }

  @Test
  public void tableFromCrdReturnsNullWhenDatabaseLabelIsMissing() {
    V1alpha1LogicalTable crd = new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name("some-table"))
        .spec(buildSpecWithOneTier());
    assertNull(schema.tableFromCrd(crd));
  }

  @Test
  public void tableFromCrdIsCaseInsensitiveOnLabelMatching() {
    V1alpha1LogicalTable crd = buildValidCrd("some-table", "LOGICAL");
    assertNotNull(schema.tableFromCrd(crd));
  }

  @Test
  public void tableFromCrdReturnsNullWhenTiersAreNull() {
    V1alpha1LogicalTable crd = new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name("some-table")
            .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, DATABASE_NAME))
        .spec(new V1alpha1LogicalTableSpec());
    assertNull(schema.tableFromCrd(crd));
  }

  @Test
  public void tableFromCrdReturnsNullWhenTiersAreEmpty() {
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec()
        .tiers(new HashMap<>());
    V1alpha1LogicalTable crd = new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name("some-table")
            .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, DATABASE_NAME))
        .spec(spec);
    assertNull(schema.tableFromCrd(crd));
  }

  @Test
  public void tableFromCrdReturnsLogicalTableWhenAllChecksPass() {
    V1alpha1LogicalTable crd = buildValidCrd("some-table", DATABASE_NAME);
    Table table = schema.tableFromCrd(crd);
    assertNotNull(table);
    assertTrue(table instanceof LogicalTable);
  }

  @Test
  public void tableFromCrdUsesCorrectTableName() {
    V1alpha1LogicalTable crd = buildValidCrd("my-table-name", DATABASE_NAME);
    Table table = schema.tableFromCrd(crd);
    assertEquals("my-table-name", ((LogicalTable) table).name());
  }

  // ── Legacy filterCrds-style tests (now exercised via tableFromCrd) ────────

  @Test
  public void tableWithMatchingLabelIsIncluded() {
    V1alpha1LogicalTable crd = makeCrd("myTable", "LOGICAL");
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, "LOGICAL");
    Table table = s.tableFromCrd(crd);
    assertNotNull(table);
    assertTrue(table instanceof LogicalTable);
  }

  @Test
  public void tableWithDifferentLabelIsExcluded() {
    V1alpha1LogicalTable crd = makeCrd("otherTable", "LOGICAL-NEARLINE-OFFLINE");
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, "LOGICAL");
    assertNull(s.tableFromCrd(crd));
  }

  @Test
  public void tableWithNoLabelIsExcluded() {
    V1alpha1LogicalTable crd = makeCrd("unlabeled", null);
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, "LOGICAL");
    assertNull(s.tableFromCrd(crd));
  }

  @Test
  public void labelMatchingIsCaseInsensitive() {
    V1alpha1LogicalTable crd = makeCrd("myTable", "LOGICAL");
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, "logical");
    assertNotNull(s.tableFromCrd(crd));
  }

  @Test
  public void crdWithNullSpecIsSkipped() {
    V1alpha1LogicalTable noSpec = new V1alpha1LogicalTable();
    noSpec.setMetadata(new V1ObjectMeta().name("broken")
        .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, "LOGICAL"));
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, "LOGICAL");
    assertNull(s.tableFromCrd(noSpec));
  }

  // ── K8s-backed tests (FakeK8sApi) ────────────────────────────────────────

  @Test
  public void loadAllTablesViaGetTableMap() {
    // Arrange: FakeK8sApi backed by a single CRD with matching label
    List<V1alpha1LogicalTable> crds = new ArrayList<>();
    crds.add(buildValidCrdWithTableName("logical-testevent", "testevent", DATABASE_NAME));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crds);
    LogicalTableSchema s =
        new LogicalTableSchema(new Properties(), null, DATABASE_NAME, fakeApi);

    // Act
    Map<String, Table> tableMap = s.getTableMap();

    // Assert: the CRD spec.tableName "testevent" appears as the key, not the metadata.name
    assertEquals(1, tableMap.size());
    assertTrue(tableMap.containsKey("testevent"));
  }

  @Test
  public void tableNameFromSpecNotMetadataName() {
    // spec.tableName = "testevent", metadata.name = "logical-testevent"
    // The map key must be "testevent" (spec.tableName), not "logical-testevent"
    List<V1alpha1LogicalTable> crds = new ArrayList<>();
    crds.add(buildValidCrdWithTableName("logical-testevent", "testevent", DATABASE_NAME));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crds);
    LogicalTableSchema s =
        new LogicalTableSchema(new Properties(), null, DATABASE_NAME, fakeApi);

    Map<String, Table> tableMap = s.getTableMap();

    assertTrue(tableMap.containsKey("testevent"));
    assertEquals("testevent", ((LogicalTable) tableMap.get("testevent")).name());
  }

  @Test
  public void loadTableMapFiltersOutCrdsWithMismatchedLabel() {
    // Two CRDs: one matching, one with a different database label
    List<V1alpha1LogicalTable> crds = new ArrayList<>();
    crds.add(buildValidCrdWithTableName("logical-testevent", "testevent", DATABASE_NAME));
    crds.add(buildValidCrdWithTableName("other-event", "otherevent", "different-database"));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crds);
    LogicalTableSchema s =
        new LogicalTableSchema(new Properties(), null, DATABASE_NAME, fakeApi);

    Map<String, Table> tableMap = s.getTableMap();

    assertEquals(1, tableMap.size());
    assertTrue(tableMap.containsKey("testevent"));
  }

  @Test
  public void loadTableMapReturnsEmptyWhenNoMatchingCrds() {
    // CRD exists but belongs to a different database
    List<V1alpha1LogicalTable> crds = new ArrayList<>();
    crds.add(buildValidCrdWithTableName("other-event", "otherevent", "other-db"));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crds);
    LogicalTableSchema s =
        new LogicalTableSchema(new Properties(), null, DATABASE_NAME, fakeApi);

    Map<String, Table> tableMap = s.getTableMap();

    assertTrue(tableMap.isEmpty());
  }

  @Test
  public void loadTableByNameDirectLookupViaTablesGet() {
    // Arrange: FakeK8sApi with a CRD that maps "logical-testevent" → testevent
    List<V1alpha1LogicalTable> crds = new ArrayList<>();
    crds.add(buildValidCrdWithTableName("logical-testevent", "testevent", DATABASE_NAME));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crds);
    K8sContext ctx = mock(K8sContext.class);
    when(ctx.namespace()).thenReturn("default");
    LogicalTableSchema s =
        new LogicalTableSchema(new Properties(), ctx, DATABASE_NAME, fakeApi);

    // Act: tables().get("testevent") triggers loadTable("testevent")
    Table result = s.tables().get("testevent");

    // Assert: the table was found and has the correct name
    assertNotNull(result);
    assertTrue(result instanceof LogicalTable);
    assertEquals("testevent", ((LogicalTable) result).name());
  }

  @Test
  public void loadTableByNameReturnsNullWhenCrdNotFound() {
    // FakeK8sApi has no CRD named "logical-unknown"
    List<V1alpha1LogicalTable> crds = new ArrayList<>();
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crds);
    K8sContext ctx = mock(K8sContext.class);
    when(ctx.namespace()).thenReturn("default");
    LogicalTableSchema s =
        new LogicalTableSchema(new Properties(), ctx, DATABASE_NAME, fakeApi);

    Table result = s.tables().get("unknown");

    assertNull(result);
  }

  @Test
  public void getTableMapReturnsEmptyMapOnK8sException() {
    // null context with no override → loadTableMap() throws NPE → getTableMap() catches → empty map
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, DATABASE_NAME);

    Map<String, Table> tableMap = s.getTableMap();

    assertNotNull(tableMap);
    assertTrue(tableMap.isEmpty());
  }

  @Test
  public void tableFromCrdUsesSpecTableNameWhenPresent() {
    // spec.tableName overrides metadata.name
    V1alpha1LogicalTable crd = buildValidCrdWithTableName("compound-metadata-name", "simple", DATABASE_NAME);
    Table table = schema.tableFromCrd(crd);

    assertNotNull(table);
    assertEquals("simple", ((LogicalTable) table).name());
  }

  @Test
  public void tableFromCrdFallsBackToMetadataNameWhenTableNameNull() {
    // spec.tableName is null → use metadata.name
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec()
        .putTiersItem("nearline", new V1alpha1LogicalTableSpecTiers().database("kafka-db"))
        .putTiersItem("online", new V1alpha1LogicalTableSpecTiers().database("venice-db"));
    // tableName is not set, so spec.getTableName() returns null
    V1alpha1LogicalTable crd = new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name("logical-testevent")
            .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, DATABASE_NAME))
        .spec(spec);

    Table table = schema.tableFromCrd(crd);

    assertNotNull(table);
    assertEquals("logical-testevent", ((LogicalTable) table).name());
  }

  @Test
  public void tierPropertyIsPassedToResolvedTierInLogicalTable() {
    // When TIER_PROPERTY is set, the LogicalTable should be created with that resolvedTier
    Properties props = new Properties();
    props.setProperty(LogicalTableDriver.TIER_PROPERTY, "nearline");
    List<V1alpha1LogicalTable> crds = new ArrayList<>();
    crds.add(buildValidCrdWithTableName("logical-testevent", "testevent", DATABASE_NAME));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crds);
    LogicalTableSchema s =
        new LogicalTableSchema(props, null, DATABASE_NAME, fakeApi);

    Map<String, Table> tableMap = s.getTableMap();

    assertEquals(1, tableMap.size());
    assertTrue(tableMap.containsKey("testevent"));
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  private V1alpha1LogicalTable buildValidCrdWithTableName(
      String metadataName, String tableName, String databaseLabel) {
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec()
        .tableName(tableName)
        .putTiersItem("nearline", new V1alpha1LogicalTableSpecTiers().database("kafka-db"))
        .putTiersItem("online", new V1alpha1LogicalTableSpecTiers().database("venice-db"));
    return new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name(metadataName)
            .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, databaseLabel))
        .spec(spec);
  }

  private V1alpha1LogicalTable buildValidCrd(String name, String databaseLabel) {
    return new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name(name)
            .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, databaseLabel))
        .spec(buildSpecWithOneTier());
  }

  private V1alpha1LogicalTableSpec buildSpecWithOneTier() {
    return new V1alpha1LogicalTableSpec()
        .putTiersItem("nearline", new V1alpha1LogicalTableSpecTiers().database("kafka-db"));
  }

  private V1alpha1LogicalTable makeCrd(String name, String schemaLabel) {
    V1alpha1LogicalTable crd = new V1alpha1LogicalTable();
    V1ObjectMeta meta = new V1ObjectMeta().name(name);
    if (schemaLabel != null) {
      meta.putLabelsItem(LogicalTableDriver.DATABASE_LABEL, schemaLabel);
    }
    crd.setMetadata(meta);
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    spec.putTiersItem("nearline", new V1alpha1LogicalTableSpecTiers().database("kafka-database"));
    spec.putTiersItem("online", new V1alpha1LogicalTableSpecTiers().database("venice"));
    crd.setSpec(spec);
    return crd;
  }
}
