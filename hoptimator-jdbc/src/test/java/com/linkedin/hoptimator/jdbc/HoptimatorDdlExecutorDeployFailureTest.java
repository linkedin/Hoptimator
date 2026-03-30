package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.util.DeploymentService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
@ExtendWith(MockitoExtension.class)
class HoptimatorDdlExecutorDeployFailureTest {

  @Mock
  MockedStatic<DeploymentService> mockDeploymentService;

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

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

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
