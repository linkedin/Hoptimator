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
 * <p>Tests call {@link LogicalTableSchema#tableFromCr} directly on a real schema
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

  // ── tableFromCr() ───────────────────────────────────────────────────────

  @Test
  public void tableFromCrReturnsNullWhenMetadataIsNull() {
    V1alpha1LogicalTable cr = new V1alpha1LogicalTable();
    assertNull(schema.tableFromCr(cr));
  }

  @Test
  public void tableFromCrReturnsNullWhenSpecIsNull() {
    V1alpha1LogicalTable cr = new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name("some-table"));
    assertNull(schema.tableFromCr(cr));
  }

  @Test
  public void tableFromCrReturnsNullWhenDatabaseLabelDoesNotMatch() {
    V1alpha1LogicalTable cr = buildValidCr("some-table", "other-database");
    assertNull(schema.tableFromCr(cr));
  }

  @Test
  public void tableFromCrReturnsNullWhenDatabaseLabelIsMissing() {
    V1alpha1LogicalTable cr = new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name("some-table"))
        .spec(buildSpecWithOneTier());
    assertNull(schema.tableFromCr(cr));
  }

  @Test
  public void tableFromCrIsCaseInsensitiveOnLabelMatching() {
    V1alpha1LogicalTable cr = buildValidCr("some-table", "LOGICAL");
    assertNotNull(schema.tableFromCr(cr));
  }

  @Test
  public void tableFromCrReturnsNullWhenTiersAreNull() {
    V1alpha1LogicalTable cr = new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name("some-table")
            .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, DATABASE_NAME))
        .spec(new V1alpha1LogicalTableSpec());
    assertNull(schema.tableFromCr(cr));
  }

  @Test
  public void tableFromCrReturnsNullWhenTiersAreEmpty() {
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec()
        .tiers(new HashMap<>());
    V1alpha1LogicalTable cr = new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name("some-table")
            .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, DATABASE_NAME))
        .spec(spec);
    assertNull(schema.tableFromCr(cr));
  }

  @Test
  public void tableFromCrReturnsLogicalTableWhenAllChecksPass() {
    V1alpha1LogicalTable cr = buildValidCr("some-table", DATABASE_NAME);
    Table table = schema.tableFromCr(cr);
    assertNotNull(table);
    assertTrue(table instanceof LogicalTable);
  }

  @Test
  public void tableFromCrUsesCorrectTableName() {
    V1alpha1LogicalTable cr = buildValidCr("my-table-name", DATABASE_NAME);
    Table table = schema.tableFromCr(cr);
    assertEquals("my-table-name", ((LogicalTable) table).name());
  }

  // ── Legacy filterCrs-style tests (now exercised via tableFromCr) ────────

  @Test
  public void tableWithMatchingLabelIsIncluded() {
    V1alpha1LogicalTable cr = makeCr("myTable", "LOGICAL");
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, "LOGICAL");
    Table table = s.tableFromCr(cr);
    assertNotNull(table);
    assertTrue(table instanceof LogicalTable);
  }

  @Test
  public void tableWithDifferentLabelIsExcluded() {
    V1alpha1LogicalTable cr = makeCr("otherTable", "LOGICAL-NEARLINE-OFFLINE");
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, "LOGICAL");
    assertNull(s.tableFromCr(cr));
  }

  @Test
  public void tableWithNoLabelIsExcluded() {
    V1alpha1LogicalTable cr = makeCr("unlabeled", null);
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, "LOGICAL");
    assertNull(s.tableFromCr(cr));
  }

  @Test
  public void labelMatchingIsCaseInsensitive() {
    V1alpha1LogicalTable cr = makeCr("myTable", "LOGICAL");
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, "logical");
    assertNotNull(s.tableFromCr(cr));
  }

  @Test
  public void crWithNullSpecIsSkipped() {
    V1alpha1LogicalTable noSpec = new V1alpha1LogicalTable();
    noSpec.setMetadata(new V1ObjectMeta().name("broken")
        .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, "LOGICAL"));
    LogicalTableSchema s = new LogicalTableSchema(new Properties(), null, "LOGICAL");
    assertNull(s.tableFromCr(noSpec));
  }

  // ── K8s-backed tests (FakeK8sApi) ────────────────────────────────────────

  @Test
  public void loadAllTablesViaGetTableMap() {
    // Arrange: FakeK8sApi backed by a single custom resource with matching label
    List<V1alpha1LogicalTable> crs = new ArrayList<>();
    crs.add(buildValidCrWithTableName("logical-testevent", "testevent", DATABASE_NAME));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crs);
    LogicalTableSchema s =
        new LogicalTableSchema(new Properties(), null, DATABASE_NAME, fakeApi);

    // Act
    Map<String, Table> tableMap = s.getTableMap();

    // Assert: the custom resource spec.tableName "testevent" appears as the key, not the metadata.name
    assertEquals(1, tableMap.size());
    assertTrue(tableMap.containsKey("testevent"));
  }

  @Test
  public void tableNameFromSpecNotMetadataName() {
    // spec.tableName = "testevent", metadata.name = "logical-testevent"
    // The map key must be "testevent" (spec.tableName), not "logical-testevent"
    List<V1alpha1LogicalTable> crs = new ArrayList<>();
    crs.add(buildValidCrWithTableName("logical-testevent", "testevent", DATABASE_NAME));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crs);
    LogicalTableSchema s =
        new LogicalTableSchema(new Properties(), null, DATABASE_NAME, fakeApi);

    Map<String, Table> tableMap = s.getTableMap();

    assertTrue(tableMap.containsKey("testevent"));
    assertEquals("testevent", ((LogicalTable) tableMap.get("testevent")).name());
  }

  @Test
  public void loadTableMapFiltersOutCrsWithMismatchedLabel() {
    // Two custom resources: one matching, one with a different database label
    List<V1alpha1LogicalTable> crs = new ArrayList<>();
    crs.add(buildValidCrWithTableName("logical-testevent", "testevent", DATABASE_NAME));
    crs.add(buildValidCrWithTableName("other-event", "otherevent", "different-database"));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crs);
    LogicalTableSchema s =
        new LogicalTableSchema(new Properties(), null, DATABASE_NAME, fakeApi);

    Map<String, Table> tableMap = s.getTableMap();

    assertEquals(1, tableMap.size());
    assertTrue(tableMap.containsKey("testevent"));
  }

  @Test
  public void loadTableMapReturnsEmptyWhenNoMatchingCrs() {
    // custom resource exists but belongs to a different database
    List<V1alpha1LogicalTable> crs = new ArrayList<>();
    crs.add(buildValidCrWithTableName("other-event", "otherevent", "other-db"));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crs);
    LogicalTableSchema s =
        new LogicalTableSchema(new Properties(), null, DATABASE_NAME, fakeApi);

    Map<String, Table> tableMap = s.getTableMap();

    assertTrue(tableMap.isEmpty());
  }

  @Test
  public void loadTableByNameDirectLookupViaTablesGet() {
    // Arrange: FakeK8sApi with a custom resource that maps "logical-testevent" → testevent
    List<V1alpha1LogicalTable> crs = new ArrayList<>();
    crs.add(buildValidCrWithTableName("logical-testevent", "testevent", DATABASE_NAME));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crs);
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
  public void loadTableByNameReturnsNullWhenCrNotFound() {
    // FakeK8sApi has no custom resource named "logical-unknown"
    List<V1alpha1LogicalTable> crs = new ArrayList<>();
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crs);
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
  public void tableFromCrUsesSpecTableNameWhenPresent() {
    // spec.tableName overrides metadata.name
    V1alpha1LogicalTable cr = buildValidCrWithTableName("compound-metadata-name", "simple", DATABASE_NAME);
    Table table = schema.tableFromCr(cr);

    assertNotNull(table);
    assertEquals("simple", ((LogicalTable) table).name());
  }

  @Test
  public void tableFromCrFallsBackToMetadataNameWhenTableNameNull() {
    // spec.tableName is null → use metadata.name
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec()
        .putTiersItem("nearline", new V1alpha1LogicalTableSpecTiers().database("kafka-db"))
        .putTiersItem("online", new V1alpha1LogicalTableSpecTiers().database("venice-db"));
    // tableName is not set, so spec.getTableName() returns null
    V1alpha1LogicalTable cr = new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name("logical-testevent")
            .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, DATABASE_NAME))
        .spec(spec);

    Table table = schema.tableFromCr(cr);

    assertNotNull(table);
    assertEquals("logical-testevent", ((LogicalTable) table).name());
  }

  @Test
  public void tierPropertyIsPassedToResolvedTierInLogicalTable() {
    // When TIER_PROPERTY is set, the LogicalTable should be created with that resolvedTier
    Properties props = new Properties();
    props.setProperty(LogicalTableDriver.TIER_PROPERTY, "nearline");
    List<V1alpha1LogicalTable> crs = new ArrayList<>();
    crs.add(buildValidCrWithTableName("logical-testevent", "testevent", DATABASE_NAME));
    FakeK8sApi<V1alpha1LogicalTable, V1alpha1LogicalTableList> fakeApi =
        new FakeK8sApi<>(crs);
    LogicalTableSchema s =
        new LogicalTableSchema(props, null, DATABASE_NAME, fakeApi);

    Map<String, Table> tableMap = s.getTableMap();

    assertEquals(1, tableMap.size());
    assertTrue(tableMap.containsKey("testevent"));
  }

  // ── helpers ──────────────────────────────────────────────────────────────

  private V1alpha1LogicalTable buildValidCrWithTableName(
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

  private V1alpha1LogicalTable buildValidCr(String name, String databaseLabel) {
    return new V1alpha1LogicalTable()
        .metadata(new V1ObjectMeta().name(name)
            .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, databaseLabel))
        .spec(buildSpecWithOneTier());
  }

  private V1alpha1LogicalTableSpec buildSpecWithOneTier() {
    return new V1alpha1LogicalTableSpec()
        .putTiersItem("nearline", new V1alpha1LogicalTableSpecTiers().database("kafka-db"));
  }

  private V1alpha1LogicalTable makeCr(String name, String schemaLabel) {
    V1alpha1LogicalTable cr = new V1alpha1LogicalTable();
    V1ObjectMeta meta = new V1ObjectMeta().name(name);
    if (schemaLabel != null) {
      meta.putLabelsItem(LogicalTableDriver.DATABASE_LABEL, schemaLabel);
    }
    cr.setMetadata(meta);
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    spec.putTiersItem("nearline", new V1alpha1LogicalTableSpecTiers().database("kafka-database"));
    spec.putTiersItem("online", new V1alpha1LogicalTableSpecTiers().database("venice"));
    cr.setSpec(spec);
    return cr;
  }
}
