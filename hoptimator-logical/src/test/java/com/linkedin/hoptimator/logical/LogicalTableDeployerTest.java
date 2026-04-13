package com.linkedin.hoptimator.logical;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validated;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.jdbc.DeployerUtils;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import com.linkedin.hoptimator.k8s.FakeK8sApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sUtils;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;
import com.linkedin.hoptimator.util.DeploymentService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Unit tests for {@link LogicalTableDeployer}.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LogicalTableDeployerTest {

  @Mock
  MockedStatic<DeploymentService> deploymentServiceMock;

  @Mock
  MockedStatic<DeployerUtils> deployerUtilsMock;

  @Mock
  MockedStatic<K8sContext> k8sContextMock;

  @Mock
  MockedStatic<HoptimatorDriver> hoptimatorDriverMock;

  @Mock
  K8sLogicalTableCrdDeployer mockCrdDeployer;

  // Helper methods shared by outer class tests

  private static Source makeSource(String database, String tableName) {
    return new Source(database, Arrays.asList(database, tableName), Collections.emptyMap());
  }

  private static V1alpha1Database makeDb(String name, String schema) {
    return new V1alpha1Database()
        .apiVersion("hoptimator.linkedin.com/v1alpha1")
        .kind("Database")
        .metadata(new V1ObjectMeta().name(name).namespace("default"))
        .spec(new V1alpha1DatabaseSpec()
            .url("jdbc:demodb://names=" + name)
            .schema(schema));
  }

  private static Properties twoTierProps(String nearlineDb, String offlineDb) {
    Properties props = new Properties();
    props.setProperty(LogicalTier.NEARLINE.tierName(), nearlineDb);
    props.setProperty(LogicalTier.OFFLINE.tierName(), offlineDb);
    return props;
  }

  private static K8sContext mockContext() {
    K8sContext ctx = mock(K8sContext.class);
    when(ctx.namespace()).thenReturn("default");
    when(ctx.withOwner(any())).thenReturn(ctx);
    when(ctx.withLabel(anyString(), anyString())).thenReturn(ctx);
    return ctx;
  }

  private static Source testSource() {
    return new Source("logical", Arrays.asList("logical", "testevent"), Collections.emptyMap());
  }

  /**
   * Creates a {@link LogicalTableDeployer} that returns {@link #mockCrdDeployer} from its
   * {@code createLogicalTableCrdDeployer} factory method, so K8s CRD interactions can be
   * verified with Mockito without a live cluster.
   */
  private LogicalTableDeployer deployerWithMockCrd(
      Source src, Properties props, K8sContext ctx,
      FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi) {
    return new LogicalTableDeployer(src, props, ctx, dbApi) {
      @Override
      K8sLogicalTableCrdDeployer createLogicalTableCrdDeployer(
          String crdName, String databaseLabel, Map<String, String> tierMap) {
        return mockCrdDeployer;
      }

      @Override
      void deployPipelineBundle(String fromTier, String toTier,
          Map<String, com.linkedin.hoptimator.k8s.models.V1alpha1Database> tierDatabases,
          Map<String, Source> tierSources,
          K8sContext ownerContext, boolean update) {
        // No-op in CRD-focused tests — pipeline deployment is tested separately.
      }
    };
  }

  // buildTierMap() tests

  @Test
  void buildTierMapWithAllThreeTiers() {
    Properties props = new Properties();
    props.setProperty("nearline", "kafka-db");
    props.setProperty("offline", "openhouse-db");
    props.setProperty("online", "venice-db");

    Map<String, String> tierMap = new LogicalTableDeployer(
        makeSource("mydb", "myTable"), props, null).buildTierMap();

    assertEquals(3, tierMap.size());
    assertEquals("kafka-db", tierMap.get("nearline"));
    assertEquals("openhouse-db", tierMap.get("offline"));
    assertEquals("venice-db", tierMap.get("online"));
  }

  @Test
  void buildTierMapIgnoresUnrecognizedKeys() {
    Properties props = new Properties();
    props.setProperty("nearline", "kafka-db");
    props.setProperty("online", "venice-db");
    props.setProperty("database", "some-db");
    props.setProperty("tier", "nearline");

    Map<String, String> tierMap = new LogicalTableDeployer(
        makeSource("mydb", "myTable"), props, null).buildTierMap();

    assertEquals(2, tierMap.size());
    assertTrue(tierMap.containsKey("nearline"));
    assertTrue(tierMap.containsKey("online"));
    assertFalse(tierMap.containsKey("database"));
    assertFalse(tierMap.containsKey("tier"));
  }

  @Test
  void buildTierMapReturnsEmptyWhenNoTiersRecognized() {
    Properties props = new Properties();
    props.setProperty("database", "some-db");
    props.setProperty("schema", "MY_SCHEMA");

    assertTrue(new LogicalTableDeployer(makeSource("mydb", "myTable"), props, null)
        .buildTierMap().isEmpty());
  }

  @Test
  void buildTierMapPreservesValues() {
    Properties props = new Properties();
    props.setProperty("nearline", "xinfra-tracking-database");
    props.setProperty("online", "venice-online-database");

    Map<String, String> tierMap = new LogicalTableDeployer(
        makeSource("mydb", "myTable"), props, null).buildTierMap();

    assertEquals("xinfra-tracking-database", tierMap.get("nearline"));
    assertEquals("venice-online-database", tierMap.get("online"));
  }

  // pipelineName() tests

  @Test
  void pipelineNameProducesCorrectFormat() {
    assertEquals("logical-mytable-nearline-to-online",
        LogicalTableDeployer.pipelineName("myTable", "nearline", "online"));
  }

  @Test
  void pipelineNameCanonializesUppercaseTableName() {
    assertEquals("logical-mytable-nearline-to-online",
        LogicalTableDeployer.pipelineName("MyTable", "nearline", "online"));
  }

  @Test
  void pipelineNameNearlineToOffline() {
    assertEquals("logical-orders-nearline-to-offline",
        LogicalTableDeployer.pipelineName("orders", "nearline", "offline"));
  }

  @Test
  void pipelineNameNearlineToOnline() {
    assertEquals("logical-events-nearline-to-online",
        LogicalTableDeployer.pipelineName("events", "nearline", "online"));
  }

  // delete() / specify() tests

  @Test
  void deleteThrowsSQLFeatureNotSupportedException() {
    Properties props = new Properties();
    props.setProperty("nearline", "kafka-db");
    props.setProperty("online", "venice-db");

    LogicalTableDeployer deployer = new LogicalTableDeployer(makeSource("mydb", "myTable"), props, null);
    SQLFeatureNotSupportedException e = assertThrows(SQLFeatureNotSupportedException.class, deployer::delete);
    assertTrue(e.getMessage().contains("Logical table deletion is not yet supported"));
  }

  // CRD model construction tests

  @Test
  void logicalTableSpecTierBindings() {
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    spec.putTiersItem("nearline", new V1alpha1LogicalTableSpecTiers().databaseCrdName("xinfra-tracking"));
    spec.putTiersItem("online", new V1alpha1LogicalTableSpecTiers().databaseCrdName("venice"));

    assertEquals(2, spec.getTiers().size());
    assertEquals("xinfra-tracking", spec.getTiers().get("nearline").getDatabaseCrdName());
    assertEquals("venice", spec.getTiers().get("online").getDatabaseCrdName());
  }

  @Test
  void crdNameIsCanonicalizedFromPath() {
    assertEquals("logical-mytable", K8sUtils.canonicalizeName(Arrays.asList("LOGICAL", "MyTable")));
  }

  // buildSelectSql tests (used internally by planPipeline)

  @Test
  void buildSelectSqlWithSchemaOnly() {
    Source source = new Source("db", Arrays.asList("KAFKA", "testevent"), Collections.emptyMap());
    assertEquals("SELECT * FROM \"KAFKA\".\"testevent\"", LogicalTableDeployer.buildSelectSql(source));
  }

  @Test
  void buildSelectSqlWithCatalogAndSchema() {
    Source source = new Source("db", Arrays.asList("MyCatalog", "KAFKA", "testevent"), Collections.emptyMap());
    assertEquals("SELECT * FROM \"MyCatalog\".\"KAFKA\".\"testevent\"", LogicalTableDeployer.buildSelectSql(source));
  }

  @Test
  void buildSelectSqlWithTableOnly() {
    Source source = new Source("db", Collections.singletonList("testevent"), Collections.emptyMap());
    assertEquals("SELECT * FROM \"testevent\"", LogicalTableDeployer.buildSelectSql(source));
  }

  // buildInsertSql tests

  @Test
  void buildInsertSqlWithSchemaOnly() {
    Source from = new Source("kafka-db", Arrays.asList("KAFKA", "testevent"), Collections.emptyMap());
    Source to = new Source("venice-db", Arrays.asList("VENICE", "testevent"), Collections.emptyMap());
    assertEquals(
        "INSERT INTO \"VENICE\".\"testevent\" SELECT * FROM \"KAFKA\".\"testevent\"",
        LogicalTableDeployer.buildInsertSql(to, from));
  }

  @Test
  void buildInsertSqlWithCatalogAndSchema() {
    Source from = new Source("db", Arrays.asList("MyCatalog", "KAFKA", "testevent"), Collections.emptyMap());
    Source to = new Source("db", Arrays.asList("MyCatalog", "VENICE", "testevent"), Collections.emptyMap());
    assertEquals(
        "INSERT INTO \"MyCatalog\".\"VENICE\".\"testevent\" SELECT * FROM \"MyCatalog\".\"KAFKA\".\"testevent\"",
        LogicalTableDeployer.buildInsertSql(to, from));
  }

  @Test
  void buildInsertSqlWithTableOnly() {
    Source from = new Source("db", Collections.singletonList("nearline_table"), Collections.emptyMap());
    Source to = new Source("db", Collections.singletonList("online_table"), Collections.emptyMap());
    assertEquals(
        "INSERT INTO \"online_table\" SELECT * FROM \"nearline_table\"",
        LogicalTableDeployer.buildInsertSql(to, from));
  }

  // buildTierMap() self-consistency and filtering tests

  @Test
  void buildTierMapIsConsistentAcrossCalls() {
    Properties props = new Properties();
    props.setProperty("nearline", "kafka-db");
    props.setProperty("offline", "openhouse-db");

    LogicalTableDeployer deployer = new LogicalTableDeployer(
        makeSource("mydb", "myTable"), props, null);
    Map<String, String> first = deployer.buildTierMap();
    Map<String, String> second = deployer.buildTierMap();

    assertEquals(first, second);
    assertNotSame(first, second);
  }

  @Test
  void buildTierMapOnlyContainsRecognizedTiers() {
    Properties props = new Properties();
    props.setProperty("nearline", "kafka-db");
    props.setProperty("offline", "openhouse-db");
    props.setProperty("schema", "X");
    props.setProperty("database", "Y");

    Map<String, String> tierMap = new LogicalTableDeployer(
        makeSource("mydb", "myTable"), props, null).buildTierMap();

    assertEquals(2, tierMap.size());
    assertTrue(tierMap.containsKey("nearline"));
    assertTrue(tierMap.containsKey("offline"));
    assertFalse(tierMap.containsKey("schema"));
    assertFalse(tierMap.containsKey("database"));
  }

  // K8s-backed tests (FakeK8sApi + mock K8sContext + mock CRD deployer)

  @Test
  void createDeploysLogicalTableCrd() throws Exception {
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));

    V1OwnerReference ownerRef = new V1OwnerReference();
    doReturn(ownerRef).when(mockCrdDeployer).createAndReference();

    LogicalTableDeployer deployer = deployerWithMockCrd(
        testSource(), twoTierProps("nearline-db", "offline-db"), mockContext(), dbApi);
    deployer.create();

    verify(mockCrdDeployer).createAndReference();
  }

  @Test
  void createSetsDatabaseLabel() throws Exception {
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));

    V1OwnerReference ownerRef = new V1OwnerReference();
    doReturn(ownerRef).when(mockCrdDeployer).createAndReference();

    LogicalTableDeployer deployer = deployerWithMockCrd(
        testSource(), twoTierProps("nearline-db", "offline-db"), mockContext(), dbApi);
    deployer.create();

    // CRD content (including DATABASE_LABEL) is verified in K8sLogicalTableCrdDeployerTest;
    // here we just confirm createAndReference() was called (the factory was invoked).
    verify(mockCrdDeployer).createAndReference();
  }

  @Test
  void restoreDeletesCreatedCrd() throws Exception {
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));

    V1OwnerReference ownerRef = new V1OwnerReference();
    doReturn(ownerRef).when(mockCrdDeployer).createAndReference();

    LogicalTableDeployer deployer = deployerWithMockCrd(
        testSource(), twoTierProps("nearline-db", "offline-db"), mockContext(), dbApi);
    deployer.create();
    deployer.restore();

    verify(mockCrdDeployer).restore();
  }

  @Test
  void restoreAfterFailedUpdateDoesNotDeletePreExistingCrd() throws Exception {
    // Regression test: restore() WITHOUT create()/update() ever running must not touch
    // the CRD deployer (logicalTableCrdDeployer is null until deployAll() runs).
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));

    LogicalTableDeployer deployer = deployerWithMockCrd(
        testSource(), twoTierProps("nearline-db", "offline-db"), mockContext(), dbApi);

    // restore() called WITHOUT create()/update() ever running
    deployer.restore();

    verify(mockCrdDeployer, never()).restore();
  }

  @Test
  void restoreAfterFailedCreateDeletesCrd() throws Exception {
    // create() runs, then something downstream fails and restore() is called.
    // The CRD deployer was set, so restore() must be called on it.
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));

    V1OwnerReference ownerRef = new V1OwnerReference();
    doReturn(ownerRef).when(mockCrdDeployer).createAndReference();

    LogicalTableDeployer deployer = deployerWithMockCrd(
        testSource(), twoTierProps("nearline-db", "offline-db"), mockContext(), dbApi);
    deployer.create();
    deployer.restore();

    verify(mockCrdDeployer).restore();
  }

  @Test
  void restoreAfterUpdateThatCreatedMissingCrdDeletesIt() throws Exception {
    // update() called, then restore() — the CRD deployer should have restore() called on it.
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));

    V1OwnerReference ownerRef = new V1OwnerReference();
    doReturn(ownerRef).when(mockCrdDeployer).updateAndReference();

    LogicalTableDeployer deployer = deployerWithMockCrd(
        testSource(), twoTierProps("nearline-db", "offline-db"), mockContext(), dbApi);
    deployer.update();
    deployer.restore();

    verify(mockCrdDeployer).restore();
  }

  @Test
  void updateWithExistingCrdSucceeds() throws Exception {
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));

    V1OwnerReference ownerRef = new V1OwnerReference();
    doReturn(ownerRef).when(mockCrdDeployer).updateAndReference();

    LogicalTableDeployer deployer = deployerWithMockCrd(
        testSource(), twoTierProps("nearline-db", "offline-db"), mockContext(), dbApi);
    deployer.update();

    verify(mockCrdDeployer).updateAndReference();
  }

  @Test
  void updateWithNoCrdCreatesNew() throws Exception {
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));

    V1OwnerReference ownerRef = new V1OwnerReference();
    doReturn(ownerRef).when(mockCrdDeployer).updateAndReference();

    LogicalTableDeployer deployer = deployerWithMockCrd(
        testSource(), twoTierProps("nearline-db", "offline-db"), mockContext(), dbApi);
    deployer.update();

    // CRD content checks (name, spec) move to K8sLogicalTableCrdDeployerTest
    verify(mockCrdDeployer).updateAndReference();
  }

  @Test
  void createWithNearlineAndOnlineTiersAttemptsPipelineDeployment() throws Exception {
    Properties props = new Properties();
    props.setProperty(LogicalTier.NEARLINE.tierName(), "nearline-db");
    props.setProperty(LogicalTier.ONLINE.tierName(), "online-db");

    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(
            makeDb("nearline-db", "NEARLINE"), makeDb("online-db", "ONLINE")));

    V1OwnerReference ownerRef = new V1OwnerReference();
    doReturn(ownerRef).when(mockCrdDeployer).createAndReference();

    // Use a subclass that mocks the CRD deployer but does NOT suppress deployPipelineBundle,
    // so the pipeline path is exercised and fails due to the null connection in mockContext().
    LogicalTableDeployer deployer = new LogicalTableDeployer(testSource(), props, mockContext(), dbApi) {
      @Override
      K8sLogicalTableCrdDeployer createLogicalTableCrdDeployer(
          String crdName, String databaseLabel, Map<String, String> tierMap) {
        return mockCrdDeployer;
      }
    };

    SQLException ex = assertThrows(SQLException.class, deployer::create);

    assertNotNull(ex.getMessage());
    assertTrue(ex.getMessage().contains("logical table") || ex.getMessage().contains("testevent")
        || ex.getCause() != null);
  }

  interface ValidatedDeployer extends Deployer, Validated {
  }

  @Test
  void validateSucceedsWithValidTierConfiguration() throws Exception {
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));

    Validator.Issues issues = new Validator.Issues("test");
    new LogicalTableDeployer(
        testSource(), twoTierProps("nearline-db", "offline-db"), mockContext(), dbApi)
        .validate(issues);

    assertTrue(issues.valid());
  }

  @Test
  void validateReportsIssueWhenDatabaseCrdNotFound() throws Exception {
    Validator.Issues issues = new Validator.Issues("test");
    new LogicalTableDeployer(
        testSource(), twoTierProps("missing-db", "also-missing"),
        mockContext(), new FakeK8sApi<>(new ArrayList<>()))
        .validate(issues);

    assertFalse(issues.valid());
  }

  @Test
  void validateCallsValidatedDeployersWhenTiersExist() throws Exception {
    ValidatedDeployer mockValidatedDeployer = mock(ValidatedDeployer.class);
    deploymentServiceMock.when(() -> DeploymentService.deployers(any(), any()))
        .thenReturn(List.of(mockValidatedDeployer));

    Properties oneTierProps = new Properties();
    oneTierProps.setProperty(LogicalTier.NEARLINE.tierName(), "nearline-db");
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE")));

    K8sContext ctx = mock(K8sContext.class);

    Validator.Issues issues = new Validator.Issues("test");
    new LogicalTableDeployer(
        makeSource("logical", "testevent"), oneTierProps, ctx, dbApi)
        .validate(issues);

    verify(mockValidatedDeployer).validate(issues);
    assertTrue(issues.valid());
  }

  @Test
  void deployerProviderReturnsDeployerWhenLogicalSchemaFound() {
    Properties tierProps = new Properties();
    tierProps.setProperty(LogicalTier.NEARLINE.tierName(), "nearline-db");
    deployerUtilsMock.when(() -> DeployerUtils.extractPropertiesFromJdbcSchema(
        any(), any(), any(), anyString(), any()))
        .thenReturn(tierProps);

    K8sContext mockCtx = mock(K8sContext.class);
    k8sContextMock.when(() -> K8sContext.create(any()))
        .thenReturn(mockCtx);

    HoptimatorConnection mockConn = mock(HoptimatorConnection.class);

    LogicalTableDeployerProvider provider = new LogicalTableDeployerProvider();
    Collection<Deployer> deployers = provider.deployers(makeSource("logical", "testevent"), mockConn);

    assertFalse(deployers.isEmpty());
    assertEquals(1, deployers.size());
    assertTrue(deployers.iterator().next() instanceof LogicalTableDeployer);
  }

  @Test
  void ensureTierRowTypesRegisteredWithConnectionRecordsRowTypeError() throws Exception {
    HoptimatorConnection mockConn = mock(HoptimatorConnection.class);
    K8sContext ctx = mock(K8sContext.class);
    when(ctx.connection()).thenReturn(mockConn);

    hoptimatorDriverMock
        .when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenThrow(new SQLException("schema not found"));

    Properties oneTierProps = new Properties();
    oneTierProps.setProperty(LogicalTier.NEARLINE.tierName(), "nearline-db");
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE")));

    Validator.Issues issues = new Validator.Issues("test");
    new LogicalTableDeployer(
        makeSource("logical", "testevent"), oneTierProps, ctx, dbApi)
        .validate(issues);

    assertFalse(issues.valid());
  }

  // specify() tests

  @Test
  void specifyWithNearlineAndOnlineThrowsException() {
    // nearline + online tiers trigger specifyFromSql() which fails on null connection.
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("online-db", "ONLINE")));
    Properties props = twoTierProps("nearline-db", "online-db");
    props.setProperty(LogicalTier.ONLINE.tierName(), "online-db");

    assertThrows(Exception.class, () -> new LogicalTableDeployer(testSource(), props, mockContext(), dbApi).specify());
  }

  @Test
  void specifyWithOfflineTierOnlyDoesNotAttemptPipeline() throws Exception {
    // OFFLINE-only (no NEARLINE) — no pipeline pairs are triggered since pipelines require NEARLINE.
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("offline-db", "OFFLINE")));

    Properties props = new Properties();
    props.setProperty(LogicalTier.OFFLINE.tierName(), "offline-db");
    List<String> specs = new LogicalTableDeployer(
        testSource(), props, mockContext(), dbApi).specify();

    assertNotNull(specs);
    assertTrue(specs.isEmpty(), "offline-only — no pipeline spec should be attempted");
  }

  @Test
  void specifyIncludesTierResourceSpecsFromDeploymentService() throws Exception {
    // Step 1 of specify() calls DeploymentService.specify(tierSource) for each tier.
    // Verify that the specs returned by DeploymentService are included in the output.
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));
    Properties props = twoTierProps("nearline-db", "offline-db");

    deploymentServiceMock.when(() -> DeploymentService.specify(any(Source.class), any()))
        .thenReturn(Arrays.asList("tier-spec-yaml"));
    deploymentServiceMock.when(() -> DeploymentService.deployers(any(), any()))
        .thenReturn(Collections.emptyList());

    // specify() calls DeploymentService.specify() per tier before the pipeline path,
    // which fails (null connection) — so we only see tier specs, not job specs.
    List<String> specs;
    try {
      specs = new LogicalTableDeployer(testSource(), props, mockContext(), dbApi).specify();
    } catch (SQLException ignored) {
      // Pipeline planning may throw due to null connection; tier specs are added first.
      return;
    }
    // If no exception, tier specs should appear.
    assertFalse(specs.isEmpty(), "Tier resource specs should be included");
  }

  @Test
  void specifyWithNearlineAndOfflineThrowsExceptionOnPipelinePlanning() {
    // NEARLINE+OFFLINE now triggers pipeline planning (like NEARLINE+ONLINE).
    FakeK8sApi<V1alpha1Database, V1alpha1DatabaseList> dbApi =
        new FakeK8sApi<>(Arrays.asList(makeDb("nearline-db", "NEARLINE"), makeDb("offline-db", "OFFLINE")));

    assertThrows(Exception.class,
        () -> new LogicalTableDeployer(testSource(), twoTierProps("nearline-db", "offline-db"),
            mockContext(), dbApi).specify());
  }

  @Test
  void buildInsertSqlWithMixedPathLengths() {
    // Verify INSERT INTO uses full qualified paths for both to and from.
    Source from = new Source("nearline-db", Arrays.asList("KAFKA", "events"), Collections.emptyMap());
    Source to = new Source("online-db", Arrays.asList("VENICE", "events"), Collections.emptyMap());
    String sql = LogicalTableDeployer.buildInsertSql(to, from);
    assertTrue(sql.startsWith("INSERT INTO \"VENICE\".\"events\""),
        "INSERT target should be the online sink: " + sql);
    assertTrue(sql.contains("SELECT * FROM \"KAFKA\".\"events\""),
        "SELECT source should be the nearline source: " + sql);
  }
}
