package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.jdbc.ddl.SqlCreateMaterializedView;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTable;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTrigger;
import com.linkedin.hoptimator.jdbc.ddl.SqlDropTrigger;
import com.linkedin.hoptimator.jdbc.ddl.SqlPauseTrigger;
import com.linkedin.hoptimator.jdbc.ddl.SqlResumeTrigger;
import com.linkedin.hoptimator.util.DeploymentService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;


/**
 * Tests for HoptimatorDdlExecutor that mock DeploymentService and ValidationService
 * to exercise failure paths and validation error propagation.
 */
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
@ExtendWith(MockitoExtension.class)
class HoptimatorDdlExecutorMockServiceTest {

  @Mock
  MockedStatic<DeploymentService> mockDeploymentService;

  @Mock
  MockedStatic<ValidationService> mockValidationService;

  private HoptimatorConnection connection;

  @BeforeEach
  void setUp() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    Properties props = new Properties();
    connection = (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", props);

    mockDeploymentService.when(() -> DeploymentService.deployers(any(), any()))
        .thenReturn(Collections.emptyList());
    mockDeploymentService.when(DeploymentService::providers)
        .thenReturn(Collections.emptyList());
  }

  /**
   * Stubs ValidationService.validateOrThrow to throw "validation failed" for all overloads.
   * Call this at the start of each validation-failure test.
   */
  private void stubValidationToFail() {
    // Casting to Object in the lambda forces Java's overload resolution to pick the T-object
    // overload (erased to Object) rather than the Collection overload.
    mockValidationService.when(() -> ValidationService.validateOrThrow((Object) any()))
        .thenThrow(new SQLException("validation failed"));
    // Also stub the Collection overload for callers that pass deployer collections.
    mockValidationService.when(() -> ValidationService.validateOrThrow(any(Collection.class)))
        .thenThrow(new SQLException("validation failed"));
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  // ---------------------------------------------------------------------------
  // DeploymentService failure tests
  // ---------------------------------------------------------------------------

  @Test
  void testCreateViewDeployFailureTriggersRollback() {
    mockDeploymentService.when(() -> DeploymentService.create(any()))
        .thenThrow(new RuntimeException("deploy failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE VIEW \"failView\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("deploy failed"));
  }

  @Test
  void testCreateOrReplaceViewDeployFailureTriggersRollback() {
    mockDeploymentService.when(() -> DeploymentService.update(any()))
        .thenThrow(new RuntimeException("update failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE OR REPLACE VIEW \"failView\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("update failed"));
  }

  @Test
  void testCreateTriggerDeployFailureTriggersRollback() {
    mockDeploymentService.when(() -> DeploymentService.create(any()))
        .thenThrow(new RuntimeException("trigger deploy failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TRIGGER \"failTrigger\" ON \"UTIL\".\"PRINT\" AS 'myJob'");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("trigger deploy failed"));
  }

  @Test
  void testCreateOrReplaceTriggerDeployFailureTriggersRollback() {
    mockDeploymentService.when(() -> DeploymentService.update(any()))
        .thenThrow(new RuntimeException("trigger update failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE OR REPLACE TRIGGER \"failTrigger\" ON \"UTIL\".\"PRINT\" AS 'myJob'");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("trigger update failed"));
  }

  @Test
  void testDropTriggerDeployFailure() {
    mockDeploymentService.when(() -> DeploymentService.delete(any()))
        .thenThrow(new RuntimeException("delete failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP TRIGGER \"failTrigger\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("delete failed"));
  }

  @Test
  void testDropTriggerIfExistsWithTableTriggerError() {
    mockDeploymentService.when(() -> DeploymentService.delete(any()))
        .thenThrow(new RuntimeException("Error getting TableTrigger for trigger"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "DROP TRIGGER IF EXISTS \"nonExistentTrigger\"");

    // Should succeed silently because IF EXISTS is specified and error matches
    executor.executeDdl(context, node);
  }

  @Test
  void testPauseTriggerDeployFailure() {
    mockDeploymentService.when(() -> DeploymentService.update(any()))
        .thenThrow(new RuntimeException("pause failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "PAUSE TRIGGER \"failTrigger\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("pause failed"));
  }

  @Test
  void testResumeTriggerDeployFailure() {
    mockDeploymentService.when(() -> DeploymentService.update(any()))
        .thenThrow(new RuntimeException("resume failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "RESUME TRIGGER \"failTrigger\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("resume failed"));
  }

  @Test
  void testDropViewOnViewTableDeployFailure() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addViewTableToDefaultSchema(context, "viewToFailDrop", "SELECT 1 AS \"col1\"");

    mockDeploymentService.when(() -> DeploymentService.delete(any()))
        .thenThrow(new RuntimeException("drop view failed"));

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP VIEW \"viewToFailDrop\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, dropNode));

    assertTrue(ex.getMessage().contains("drop view failed"));
  }

  @Test
  void testDropMaterializedViewDeployFailure() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addMaterializedViewToDefaultSchema(context, "mvToFailDrop", "SELECT 1 AS \"col1\"");

    mockDeploymentService.when(() -> DeploymentService.delete(any()))
        .thenThrow(new RuntimeException("drop mv failed"));

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection,
        "DROP MATERIALIZED VIEW \"mvToFailDrop\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, dropNode));

    assertTrue(ex.getMessage().contains("drop mv failed"));
  }

  @Test
  void testCreateTableDeployFailure() {
    mockDeploymentService.when(() -> DeploymentService.create(any()))
        .thenThrow(new RuntimeException("table deploy failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"failTable\" (\"col1\" VARCHAR)");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("table deploy failed"));
  }

  @Test
  void testCreateOrReplaceTableDeployFailure() {
    mockDeploymentService.when(() -> DeploymentService.update(any()))
        .thenThrow(new RuntimeException("table update failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addViewTableToDefaultSchema(context, "existTable", "SELECT 1 AS \"col1\"");

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE OR REPLACE TABLE \"existTable\" (\"col1\" VARCHAR)");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("table update failed"));
  }

  // ---------------------------------------------------------------------------
  // ValidationService failure tests
  // ---------------------------------------------------------------------------

  @Test
  void testCreateViewValidationFailure() {
    stubValidationToFail();
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlCreateView node = (SqlCreateView) HoptimatorDriver.parseQuery(connection,
        "CREATE VIEW \"vf\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }

  @Test
  void testCreateTriggerValidationFailure() {
    stubValidationToFail();
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlCreateTrigger node = (SqlCreateTrigger) HoptimatorDriver.parseQuery(connection,
        "CREATE TRIGGER \"tf\" ON \"UTIL\".\"PRINT\" AS 'myJob'");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }

  @Test
  void testCreateTableValidationFailure() {
    stubValidationToFail();
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlCreateTable node = (SqlCreateTable) HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"tf\" (\"col1\" VARCHAR)");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }

  @Test
  void testCreateMaterializedViewValidationFailure() {
    stubValidationToFail();
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlCreateMaterializedView node = (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW \"DEFAULT\".\"mvVF\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }

  @Test
  void testDropTriggerValidationFailure() {
    stubValidationToFail();
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlDropTrigger node = (SqlDropTrigger) HoptimatorDriver.parseQuery(connection,
        "DROP TRIGGER \"myTrigger\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }

  @Test
  void testDropObjectValidationFailure() {
    stubValidationToFail();
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlDropObject node = (SqlDropObject) HoptimatorDriver.parseQuery(connection,
        "DROP VIEW IF EXISTS \"someView\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }

  @Test
  void testPauseTriggerValidationFailure() {
    stubValidationToFail();
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlPauseTrigger node = (SqlPauseTrigger) HoptimatorDriver.parseQuery(connection,
        "PAUSE TRIGGER \"myTrigger\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }

  @Test
  void testResumeTriggerValidationFailure() {
    stubValidationToFail();
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlResumeTrigger node = (SqlResumeTrigger) HoptimatorDriver.parseQuery(connection,
        "RESUME TRIGGER \"myTrigger\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));
    assertTrue(ex.getMessage().contains("validation failed"));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void addViewTableToDefaultSchema(CalcitePrepare.Context context, String name, String sql) {
    HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);
    List<String> schemaPath = context.getDefaultSchemaPath();
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(name);
    ViewTable viewTable = HoptimatorDdlUtils.viewTable(context, sql, prepare, schemaPath, viewPath);
    SchemaPlus defaultSchema = context.getMutableRootSchema().plus();
    for (String p : schemaPath) {
      SchemaPlus next = defaultSchema.subSchemas().get(p);
      if (next == null) {
        throw new AssertionError("Schema path segment not found: " + p);
      }
      defaultSchema = next;
    }
    defaultSchema.add(name, viewTable);
  }

  private void addMaterializedViewToDefaultSchema(CalcitePrepare.Context context, String name, String sql) {
    HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);
    List<String> schemaPath = context.getDefaultSchemaPath();
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(name);
    ViewTable viewTable = HoptimatorDdlUtils.viewTable(context, sql, prepare, schemaPath, viewPath);
    MaterializedViewTable mvTable = new MaterializedViewTable(viewTable);
    SchemaPlus defaultSchema = context.getMutableRootSchema().plus();
    for (String p : schemaPath) {
      SchemaPlus next = defaultSchema.subSchemas().get(p);
      if (next == null) {
        throw new AssertionError("Schema path segment not found: " + p);
      }
      defaultSchema = next;
    }
    defaultSchema.add(name, mvTable);
  }
}
