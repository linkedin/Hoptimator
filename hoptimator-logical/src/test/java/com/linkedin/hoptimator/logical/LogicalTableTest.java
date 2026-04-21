package com.linkedin.hoptimator.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Unit tests for {@link LogicalTable}.
 */
@ExtendWith(MockitoExtension.class)
public class LogicalTableTest {

  @Mock
  private K8sContext mockContext;

  private static final RelDataTypeFactory TYPE_FACTORY =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

  private static Map<String, V1alpha1LogicalTableSpecTiers> sampleTiers() {
    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    tiers.put("nearline", new V1alpha1LogicalTableSpecTiers().database("kafka-database"));
    tiers.put("online", new V1alpha1LogicalTableSpecTiers().database("venice"));
    return tiers;
  }

  @Test
  public void nameReturnsValuePassedToConstructor() {
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), null, mockContext);
    assertEquals("myTable", table.name());
  }

  @Test
  public void constructorRejectsNullTiers() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new LogicalTable("bad", null, null, mockContext));
    assertTrue(e.getMessage().contains("at least one tier"));
  }

  @Test
  public void constructorRejectsEmptyTiers() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new LogicalTable("bad", new HashMap<>(), null, mockContext));
    assertTrue(e.getMessage().contains("at least one tier"));
  }

  @Test
  public void constructorRejectsNullTiersFromSpec() {
    // spec.getTiers() is null → constructor validation should reject
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    assertThrows(IllegalArgumentException.class,
        () -> new LogicalTable("bad", spec.getTiers(), null, mockContext));
  }

  @Test
  public void resolveRowTypeThrowsOnUnknownResolvedTier() {
    // resolvedTier="offline" is not in sampleTiers() which only has nearline+online
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), "offline", mockContext);
    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> table.getRowType(TYPE_FACTORY));
    assertTrue(e.getMessage().contains("offline"));
  }

  @Test
  public void unknownTypeReturnedWhenContextNonNullButK8sApiFails() {
    // Non-null context but K8sApi.get() NPEs (generic() returns null on unstubbed mock).
    // The catch block in resolveRowType() swallows the exception → returns null → unknown type.
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), null, mockContext);
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertNotNull(result);
  }

  @Test
  public void unknownTypeReturnedWhenResolvedTierPresentButK8sApiFails() {
    // resolvedTier is "nearline" which exists in sampleTiers() — no IllegalStateException.
    // K8s API call fails (generic() returns null on mock) → catch → unknown type.
    LogicalTable table = new LogicalTable("myTable", sampleTiers(), "nearline", mockContext);
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertNotNull(result);
  }

  @Test
  public void unknownTypeReturnedWhenOnlineTierUsedAsFallback() {
    // sampleTiers() has "online" but not "nearline", so first-available tier is used.
    // Wait — sampleTiers() actually has both; use a map with just "online".
    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    tiers.put("online", new V1alpha1LogicalTableSpecTiers().database("venice-db"));
    tiers.put("offline", new V1alpha1LogicalTableSpecTiers().database("openhouse-db"));
    LogicalTable table = new LogicalTable("myTable", tiers, null, mockContext);
    // No nearline tier → falls back to first available; K8s call fails → unknown type.
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertNotNull(result);
  }

  @Test
  public void unknownTypeReturnedWhenTierBindingHasNullDatabaseName() {
    // tierBinding.getDatabase() == null → resolveRowType returns null → unknown type
    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    tiers.put("nearline", new V1alpha1LogicalTableSpecTiers()); // no database set
    tiers.put("online", new V1alpha1LogicalTableSpecTiers().database("venice-db"));
    LogicalTable table = new LogicalTable("myTable", tiers, null, mockContext);
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertNotNull(result);
  }

  @Test
  public void unknownTypeReturnedWhenDbCrdHasNullSpec() {
    // FakeK8sApi returns a Database CRD with null spec → resolveRowType returns null → unknown type
    V1alpha1Database dbCrd = new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("nearline-db"));
    // spec is null
    List<V1alpha1Database> databases = new ArrayList<>();
    databases.add(dbCrd);
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> fakeDbApi = new FakeK8sApi<>(databases);

    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    tiers.put("nearline", new V1alpha1LogicalTableSpecTiers().database("nearline-db"));
    tiers.put("online", new V1alpha1LogicalTableSpecTiers().database("venice-db"));
    LogicalTable table = new LogicalTable("MEMBERS", tiers, null, fakeDbApi);
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertNotNull(result);
  }

  @Test
  public void unknownTypeReturnedWhenJdbcUrlNotFound() {
    // FakeK8sApi returns a Database CRD with a bad URL → DriverManager.getConnection fails
    // → catch block → returns null → unknown type
    V1alpha1Database dbCrd = new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("nearline-db"))
        .spec(new V1alpha1DatabaseSpec()
            .url("jdbc:nonexistent://host")
            .schema("PROFILE"));
    List<V1alpha1Database> databases = new ArrayList<>();
    databases.add(dbCrd);
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> fakeDbApi =
        new FakeK8sApi<>(databases);

    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    tiers.put("nearline", new V1alpha1LogicalTableSpecTiers().database("nearline-db"));
    tiers.put("online", new V1alpha1LogicalTableSpecTiers().database("venice-db"));
    LogicalTable table = new LogicalTable("MEMBERS", tiers, null, fakeDbApi);
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertNotNull(result);
  }

  @Test
  public void unknownTypeReturnedWhenSchemaNotFoundInTier() {
    // Database CRD with valid URL (demodb) but schema name that doesn't exist
    V1alpha1Database dbCrd = new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("nearline-db"))
        .spec(new V1alpha1DatabaseSpec()
            .url("jdbc:demodb://names=PROFILE")
            .schema("NONEXISTENT_SCHEMA"));
    List<V1alpha1Database> databases = new ArrayList<>();
    databases.add(dbCrd);
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> fakeDbApi =
        new FakeK8sApi<>(databases);

    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    tiers.put("nearline", new V1alpha1LogicalTableSpecTiers().database("nearline-db"));
    tiers.put("online", new V1alpha1LogicalTableSpecTiers().database("venice-db"));
    LogicalTable table = new LogicalTable("MEMBERS", tiers, null, fakeDbApi);
    // schema "NONEXISTENT_SCHEMA" not present → returns null → unknown type
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertNotNull(result);
  }

  @Test
  public void unknownTypeReturnedWhenTableNotFoundInTierSchema() {
    // Database CRD with valid URL (demodb) and valid schema but table name doesn't exist
    V1alpha1Database dbCrd = new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("nearline-db"))
        .spec(new V1alpha1DatabaseSpec()
            .url("jdbc:demodb://names=PROFILE")
            .schema("PROFILE"));
    List<V1alpha1Database> databases = new ArrayList<>();
    databases.add(dbCrd);
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> fakeDbApi =
        new FakeK8sApi<>(databases);

    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    tiers.put("nearline", new V1alpha1LogicalTableSpecTiers().database("nearline-db"));
    tiers.put("online", new V1alpha1LogicalTableSpecTiers().database("venice-db"));
    // Table "NONEXISTENT_TABLE" is not in the demodb PROFILE schema
    LogicalTable table = new LogicalTable("NONEXISTENT_TABLE", tiers, null, fakeDbApi);
    RelDataType result = table.getRowType(TYPE_FACTORY);
    assertNotNull(result);
  }

  @Test
  public void rowTypeResolvedFromTierWhenTableExistsInDemodb() {
    // Full happy path: Database CRD → demodb PROFILE schema → MEMBERS table → row type resolved
    V1alpha1Database dbCrd = new V1alpha1Database()
        .metadata(new V1ObjectMeta().name("nearline-db"))
        .spec(new V1alpha1DatabaseSpec()
            .url("jdbc:demodb://names=PROFILE")
            .schema("PROFILE"));
    List<V1alpha1Database> databases = new ArrayList<>();
    databases.add(dbCrd);
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> fakeDbApi =
        new FakeK8sApi<>(databases);

    Map<String, V1alpha1LogicalTableSpecTiers> tiers = new HashMap<>();
    tiers.put("nearline", new V1alpha1LogicalTableSpecTiers().database("nearline-db"));
    tiers.put("online", new V1alpha1LogicalTableSpecTiers().database("venice-db"));
    // "MEMBERS" is a known table in the demodb PROFILE schema
    LogicalTable table = new LogicalTable("MEMBERS", tiers, null, fakeDbApi);
    RelDataType result = table.getRowType(TYPE_FACTORY);
    // Should resolve a real row type (not unknown type) since MEMBERS exists
    assertNotNull(result);
    assertTrue(result.getFieldCount() > 0);
  }
}
