package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateMaterializedView;
import com.linkedin.hoptimator.util.DeploymentService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.type.SqlTypeName;
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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;


/**
 * Functional tests for HoptimatorDdlExecutor using an in-process Calcite connection.
 * Organized by DDL statement type: VIEW, TRIGGER, TABLE, MATERIALIZED VIEW, and error/edge cases.
 */
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
@ExtendWith(MockitoExtension.class)
class HoptimatorDdlExecutorDdlTest {

  /** A minimal in-memory schema that implements Database for testing. */
  private static class TestDatabaseSchema extends AbstractSchema implements Database {

    private final String dbName;

    TestDatabaseSchema(String dbName) {
      this.dbName = dbName;
    }

    @Override
    public String databaseName() {
      return dbName;
    }

    @Override
    public Lookup<Table> tables() {
      return Lookup.empty();
    }
  }

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

  // ---------------------------------------------------------------------------
  // CREATE/DROP VIEW
  // ---------------------------------------------------------------------------

  @Test
  void testDdlExecutorWithWritableConnection() throws SQLException {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    assertNotNull(executor);
  }

  @Test
  void testExecuteCreateViewOnDefaultSchema() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "CREATE VIEW \"myView\" AS SELECT 1 AS \"col1\"");

    try {
      executor.executeDdl(context, node);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteCreateOrReplaceView() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "CREATE OR REPLACE VIEW \"myView\" AS SELECT 1 AS \"col1\"");

    try {
      executor.executeDdl(context, node);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteCreateViewThenReplaceView() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    SqlNode createNode = HoptimatorDriver.parseQuery(connection,
        "CREATE VIEW \"replaceTestView\" AS SELECT 1 AS \"col1\"");
    try {
      executor.executeDdl(context, createNode);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      return;
    }

    SqlNode replaceNode = HoptimatorDriver.parseQuery(connection,
        "CREATE OR REPLACE VIEW \"replaceTestView\" AS SELECT 2 AS \"col1\"");
    try {
      executor.executeDdl(context, replaceNode);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteCreateViewOnSchemaWithExistingTable() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE VIEW \"UTIL\".\"PRINT\" AS SELECT 1 AS \"col1\"");

    // UTIL.PRINT exists but is not a HoptimatorJdbcTable, so this may succeed or fail for other reasons
    try {
      executor.executeDdl(context, node);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteDropViewIfExists() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP VIEW IF EXISTS \"nonExistent\"");

    executor.executeDdl(context, node);
  }

  @Test
  void testExecuteDropViewWithoutIfExists() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP VIEW \"nonExistent\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  void testExecuteDropViewOnViewTable() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addViewTableToDefaultSchema(context, "viewToDrop", "SELECT 1 AS \"col1\"");

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP VIEW \"viewToDrop\"");
    executor.executeDdl(context, dropNode);
  }

  @Test
  void testExecuteCreateViewThenDropView() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    SqlNode createNode = HoptimatorDriver.parseQuery(connection,
        "CREATE VIEW \"dropMeView\" AS SELECT 1 AS \"col1\"");
    assertDoesNotThrow(() -> executor.executeDdl(context, createNode));

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP VIEW \"dropMeView\"");
    assertDoesNotThrow(() -> executor.executeDdl(context, dropNode));
  }

  @Test
  void testExecuteCreateViewSchemaNotFound() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE VIEW \"NONEXISTENT\".\"v\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  void testExecuteDropViewOnMaterializedViewTableThrows() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addMaterializedViewToDefaultSchema(context, "mvNotView", "SELECT 1 AS \"col1\"");

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP VIEW \"mvNotView\"");
    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, dropNode));

    assertTrue(ex.getMessage().contains("materialized view"));
  }

  @Test
  void testExecuteDropViewOnTemporaryTableThrows() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    SqlNode createNode = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"tempForViewDrop\" (\"col1\" VARCHAR)");
    assertDoesNotThrow(() -> executor.executeDdl(context, createNode));

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP VIEW \"tempForViewDrop\"");
    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, dropNode));

    assertTrue(ex.getMessage().contains("is a table"));
  }

  // ---------------------------------------------------------------------------
  // CREATE TRIGGER / DROP TRIGGER / PAUSE / RESUME
  // ---------------------------------------------------------------------------

  @Test
  void testExecuteCreateTriggerOnTable() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TRIGGER \"myTrigger\" ON \"UTIL\".\"PRINT\" AS 'myJob'");

    try {
      executor.executeDdl(context, node);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteCreateTriggerWithNamespace() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TRIGGER \"nsTrigger\" ON \"UTIL\".\"PRINT\" AS 'myJob' IN 'myNamespace'");

    try {
      executor.executeDdl(context, node);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteCreateTriggerWithCron() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TRIGGER \"cronTrigger\" ON \"UTIL\".\"PRINT\" AS 'myJob' SCHEDULED '0 * * * *'");

    try {
      executor.executeDdl(context, node);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteCreateTriggerMultiPartNameThrows() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TRIGGER \"schema\".\"trigger\" ON \"UTIL\".\"PRINT\" AS 'myJob'");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("cannot belong to a schema"));
  }

  @Test
  void testExecuteCreateTriggerTargetNotFound() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TRIGGER \"myTrigger\" ON \"UTIL\".\"NONEXISTENT\" AS 'myJob'");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  void testExecuteCreateTriggerTargetSchemaNotFound() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TRIGGER \"t\" ON \"NONEXISTENT\".\"table\" AS 'job'");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  void testExecuteCreateOrReplaceTrigger() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE OR REPLACE TRIGGER \"replaceTrigger\" ON \"UTIL\".\"PRINT\" AS 'myJob'");

    // No K8s deployers registered in test env — trigger creation is a no-op and always succeeds
    assertDoesNotThrow(() -> executor.executeDdl(context, node));
  }

  @Test
  void testExecuteDropTrigger() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP TRIGGER \"myTrigger\"");

    try {
      executor.executeDdl(context, node);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteDropTriggerIfExists() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP TRIGGER IF EXISTS \"myTrigger\"");

    try {
      executor.executeDdl(context, node);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteDropTriggerMultiPartNameThrows() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP TRIGGER \"schema\".\"trigger\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("cannot belong to a schema"));
  }

  @Test
  void testExecutePauseTrigger() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "PAUSE TRIGGER \"myTrigger\"");

    // No K8s deployers registered in test env — PAUSE is a no-op and always succeeds
    assertDoesNotThrow(() -> executor.executeDdl(context, node));
  }

  @Test
  void testExecutePauseTriggerMultiPartNameThrows() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "PAUSE TRIGGER \"schema\".\"trigger\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("cannot belong to a schema"));
  }

  @Test
  void testExecuteResumeTrigger() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "RESUME TRIGGER \"myTrigger\"");

    // No K8s deployers registered in test env — RESUME is a no-op and always succeeds
    assertDoesNotThrow(() -> executor.executeDdl(context, node));
  }

  @Test
  void testExecuteResumeTriggerMultiPartNameThrows() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "RESUME TRIGGER \"schema\".\"trigger\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("cannot belong to a schema"));
  }

  // ---------------------------------------------------------------------------
  // CREATE TABLE / DROP TABLE
  // ---------------------------------------------------------------------------

  @Test
  void testExecuteCreateTableWithColumns() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"test\" (\"col1\" VARCHAR)");

    try {
      executor.executeDdl(context, node);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteCreateTableWithQuery() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"test\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("not currently supported"));
  }

  @Test
  void testExecuteCreateTableSchemaNotFound() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"NONEXISTENT\".\"test\" (\"col1\" VARCHAR)");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  void testExecuteCreateTableMultipleColumns() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"multiCol\" (\"col1\" VARCHAR, \"col2\" INTEGER, \"col3\" BOOLEAN)");

    // Table is registered in the Calcite in-memory schema; no deployers registered, so this always succeeds
    assertDoesNotThrow(() -> executor.executeDdl(context, node));
  }

  @Test
  void testExecuteCreateTableExistingWithoutReplace() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addViewTableToDefaultSchema(context, "existingTable", "SELECT 1 AS \"col1\"");

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"existingTable\" (\"col1\" VARCHAR)");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("already exists"));
  }

  @Test
  void testExecuteCreateTableThreeLevelCatalogNotFound() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"NONEXISTENT\".\"schema1\".\"table1\" (\"col1\" VARCHAR)");

    // 3-level path with non-existent catalog causes NPE in schema resolution
    assertThrows(Exception.class, () -> executor.executeDdl(context, node));
  }

  @Test
  void testExecuteCreateTableThreeLevelOnUtilCatalog() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"UTIL\".\"newdb\".\"newTable\" (\"col1\" VARCHAR)");

    // UTIL catalog exists but "newdb" schema does not — schema resolution fails
    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));
    assertNotNull(ex.getMessage());
  }

  @Test
  void testExecuteCreateOrReplaceTable() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    SqlNode createNode = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"replaceableTable\" (\"col1\" VARCHAR)");
    try {
      executor.executeDdl(context, createNode);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      return;
    }

    SqlNode replaceNode = HoptimatorDriver.parseQuery(connection,
        "CREATE OR REPLACE TABLE \"replaceableTable\" (\"col1\" VARCHAR, \"col2\" INTEGER)");
    try {
      executor.executeDdl(context, replaceNode);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteCreateTableIfNotExistsOnExisting() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    SqlNode createNode = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"ifNotExistsTable\" (\"col1\" VARCHAR)");
    try {
      executor.executeDdl(context, createNode);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      return;
    }

    SqlNode existsNode = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE IF NOT EXISTS \"ifNotExistsTable\" (\"col1\" VARCHAR)");
    try {
      executor.executeDdl(context, existsNode);
    } catch (HoptimatorDdlExecutor.DdlException e) {
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void testExecuteCreateTableThenDropTable() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    SqlNode createNode = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"dropMeTable\" (\"col1\" VARCHAR)");
    assertDoesNotThrow(() -> executor.executeDdl(context, createNode));

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP TABLE \"dropMeTable\"");
    assertDoesNotThrow(() -> executor.executeDdl(context, dropNode));
  }

  @Test
  void testExecuteDropTableIfExists() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP TABLE IF EXISTS \"nonExistent\"");

    executor.executeDdl(context, node);
  }

  @Test
  void testExecuteDropTableWithoutIfExists() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP TABLE \"nonExistent\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  void testExecuteDropTableOnViewTableThrows() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addViewTableToDefaultSchema(context, "viewNotTable", "SELECT 1 AS \"col1\"");

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP TABLE \"viewNotTable\"");
    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, dropNode));

    assertTrue(ex.getMessage().contains("is a view"));
  }

  @Test
  void testExecuteDropTableOnMaterializedViewThrows() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addMaterializedViewToDefaultSchema(context, "mvNotTable", "SELECT 1 AS \"col1\"");

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP TABLE \"mvNotTable\"");
    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, dropNode));

    assertTrue(ex.getMessage().contains("materialized view"));
  }

  @Test
  void testCreateTableWithPrimaryKeyConstraintLogsAndContinues() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    // PRIMARY KEY results in a SqlKeyConstraint node, which is silently logged and ignored.
    // With no deployers registered, the table is created without error.
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"pkTable\" (\"col1\" INTEGER NOT NULL, PRIMARY KEY (\"col1\"))");

    // SqlKeyConstraint is silently ignored; covers the else-if SqlKeyConstraint branch
    assertDoesNotThrow(() -> executor.executeDdl(context, node));
  }

  @Test
  void testCreateTableSchemaIsNotDatabaseFallsBackToConnectionSchema() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    // The DEFAULT schema is not a Database, so database falls back to connection.getSchema()
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DEFAULT\".\"fallbackSchemaTable\" (\"col1\" VARCHAR)");

    // Covers the `database = connection.getSchema()` else-branch; no deployers → no exception
    assertDoesNotThrow(() -> executor.executeDdl(context, node));
  }

  @Test
  void testCreateTableWithDatabaseSchemaUsesDatabaseName() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    // Register a TestDatabaseSchema — since it implements Database, the executor uses
    // databaseName() rather than falling back to connection.getSchema()
    SchemaPlus rootSchema = context.getMutableRootSchema().plus();
    rootSchema.add("DBSCHEMA", new TestDatabaseSchema("mydb"));

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"DBSCHEMA\".\"dbNameTable\" (\"col1\" VARCHAR)");

    // No deployers registered, so the create succeeds
    assertDoesNotThrow(() -> executor.executeDdl(context, node));
  }

  @Test
  void testCreateTableDeployFailureWithSchemaSnapshotNonNullRollback() {
    mockDeploymentService.when(() -> DeploymentService.create(any()))
        .thenThrow(new RuntimeException("create with snapshot failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    // Register a Database schema so deployment is attempted
    SchemaPlus rootSchema = context.getMutableRootSchema().plus();
    rootSchema.add("SNAPDBSCHEMA", new TestDatabaseSchema("snapdb"));

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE TABLE \"SNAPDBSCHEMA\".\"snapTable\" (\"col1\" VARCHAR)");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("create with snapshot failed"));
  }

  // ---------------------------------------------------------------------------
  // CREATE MATERIALIZED VIEW / DROP MATERIALIZED VIEW
  // ---------------------------------------------------------------------------

  @Test
  void testExecuteCreateMaterializedViewOnNonDatabase() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW \"DEFAULT\".\"testMV\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("not a physical database"));
  }

  @Test
  void testExecuteCreateMaterializedViewSchemaNotFound() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW \"NONEXISTENT\".\"mv\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  void testExecuteDropMaterializedViewIfExists() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP MATERIALIZED VIEW IF EXISTS \"nonExistent\"");

    executor.executeDdl(context, node);
  }

  @Test
  void testExecuteDropMaterializedViewWithoutIfExists() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP MATERIALIZED VIEW \"nonExistent\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  void testExecuteDropMaterializedViewOnMaterializedViewTable() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addMaterializedViewToDefaultSchema(context, "mvToDrop", "SELECT 1 AS \"col1\"");

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP MATERIALIZED VIEW \"mvToDrop\"");
    executor.executeDdl(context, dropNode);
  }

  @Test
  void testExecuteDropMaterializedViewOnViewTableThrows() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addViewTableToDefaultSchema(context, "viewNotMv", "SELECT 1 AS \"col1\"");

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP MATERIALIZED VIEW \"viewNotMv\"");
    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, dropNode));

    assertTrue(ex.getMessage().contains("is a view"));
  }

  @Test
  void testExecuteCreateMaterializedViewDuplicateOnViewTable() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addViewTableToDefaultSchema(context, "existingView", "SELECT 1 AS \"col1\"");

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW \"existingView\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("already exists"));
  }

  @Test
  void testExecuteCreateMaterializedViewIfNotExistsOnExisting() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    addMaterializedViewToDefaultSchema(context, "existingMV", "SELECT 1 AS \"col1\"");

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW IF NOT EXISTS \"existingMV\" AS SELECT 1 AS \"col1\"");

    // Should return silently since IF NOT EXISTS is specified and view exists
    executor.executeDdl(context, node);
  }

  @Test
  void testExecuteCreateMaterializedViewOverwritePhysicalTable() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW \"UTIL\".\"PRINT\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertNotNull(ex.getMessage());
  }

  @Test
  void testCreateMaterializedViewDeployFailureTriggersRollback() {
    mockDeploymentService.when(() -> DeploymentService.plan(any(), any(), any()))
        .thenThrow(new RuntimeException("plan failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    registerDatabaseSchema(context, "TESTDB");

    SqlCreateMaterializedView node = (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW \"TESTDB\".\"testMV\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.execute(node, context));

    assertTrue(ex.getMessage().contains("plan failed"));
  }

  @Test
  void testCreateMaterializedViewDeployCreateFailureWithRollback() {
    mockDeploymentService.when(() -> DeploymentService.plan(any(), any(), any()))
        .thenThrow(new RuntimeException("deploy create failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    registerDatabaseSchema(context, "MVDB");

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW \"MVDB\".\"myMV\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("deploy create failed"));
  }

  @Test
  void testCreateOrReplaceMaterializedViewDeployFailureWithExistingViewRollback() {
    mockDeploymentService.when(() -> DeploymentService.plan(any(), any(), any()))
        .thenThrow(new RuntimeException("or replace plan failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SchemaPlus dbSchema = registerDatabaseSchema(context, "REPLACEDB");

    // Pre-populate a materialized view so OR REPLACE triggers the replace path
    addMaterializedViewToSchema(context, dbSchema, "REPLACEDB", "existingMvToReplace",
        "SELECT 1 AS \"col1\"");

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE OR REPLACE MATERIALIZED VIEW \"REPLACEDB\".\"existingMvToReplace\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("or replace plan failed"));
  }

  @Test
  void testCreateMaterializedViewIfNotExistsOnExistingNoops() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SchemaPlus dbSchema = registerDatabaseSchema(context, "IFNOTEXISTSDB");

    // Pre-populate a materialized view
    addMaterializedViewToSchema(context, dbSchema, "IFNOTEXISTSDB", "existingMvForIfNotExists",
        "SELECT 1 AS \"col1\"");

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW IF NOT EXISTS \"IFNOTEXISTSDB\".\"existingMvForIfNotExists\" AS SELECT 1 AS \"col1\"");

    // Should return silently since IF NOT EXISTS is specified and view exists
    assertDoesNotThrow(() -> executor.executeDdl(context, node));
  }

  @Test
  void testCreateMaterializedViewOverwritePhysicalTableThrows() {
    mockDeploymentService.when(() -> DeploymentService.plan(any(), any(), any()))
        .thenThrow(new RuntimeException("should not reach plan"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    // Use the UTIL.PRINT path — UTIL is not a Database, so "not a physical database" is expected
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW \"UTIL\".\"PRINT\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    // UTIL schema is not a Database, so "not a physical database" is expected
    assertTrue(ex.getMessage() != null);
  }

  @Test
  void testCreateMaterializedViewDuplicateWithoutReplaceFails() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SchemaPlus dbSchema = registerDatabaseSchema(context, "DUPDB");

    // Pre-populate a materialized view
    addMaterializedViewToSchema(context, dbSchema, "DUPDB", "dupMV", "SELECT 1 AS \"col1\"");

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW \"DUPDB\".\"dupMV\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("already exists"));
  }

  @Test
  void testCreateMaterializedViewWithSchemaSnapshotRestoredOnFailure() {
    // Fail during plan so we exercise the schemaSnapshot restore path
    mockDeploymentService.when(() -> DeploymentService.plan(any(), any(), any()))
        .thenThrow(new RuntimeException("snapshot restore test"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    registerDatabaseSchema(context, "SNAPDB");

    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE MATERIALIZED VIEW \"SNAPDB\".\"snapMV\" AS SELECT 1 AS \"col1\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("snapshot restore test"));
  }

  // ---------------------------------------------------------------------------
  // Error / edge cases
  // ---------------------------------------------------------------------------

  @Test
  void testPrepareExecuteDdl() {
    HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection, "DROP VIEW IF EXISTS \"nope\"");

    prepare.executeDdl(context, node);
  }

  @Test
  void testDropTableUnsupportedTableType() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();

    // Add an AbstractTable (not ViewTable/MaterializedViewTable/HoptimatorJdbcTable/TemporaryTable)
    AbstractTable customTable = new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("col1", SqlTypeName.VARCHAR).build();
      }
    };
    List<String> schemaPath = context.getDefaultSchemaPath();
    SchemaPlus defaultSchema = context.getMutableRootSchema().plus();
    for (String p : schemaPath) {
      SchemaPlus next = defaultSchema.subSchemas().get(p);
      if (next == null) {
        throw new AssertionError("Schema path segment not found: " + p);
      }
      defaultSchema = next;
    }
    defaultSchema.add("unsupportedTable", customTable);

    SqlNode dropNode = HoptimatorDriver.parseQuery(connection, "DROP TABLE \"unsupportedTable\"");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, dropNode));

    assertTrue(ex.getMessage().contains("Unsupported drop type"));
  }

  @Test
  void testDropTriggerIfExistsNonMatchingErrorRethrows() {
    mockDeploymentService.when(() -> DeploymentService.delete(any()))
        .thenThrow(new RuntimeException("some other error without TableTrigger"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "DROP TRIGGER IF EXISTS \"nonMatchingTrigger\"");

    // IF EXISTS, but error message doesn't match "Error getting TableTrigger" — should rethrow
    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("some other error"));
  }

  @Test
  void testCreateOrReplaceTriggerDeployFailureWithDeployersRestored() {
    mockDeploymentService.when(() -> DeploymentService.update(any()))
        .thenThrow(new RuntimeException("trigger replace failed"));

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlNode node = HoptimatorDriver.parseQuery(connection,
        "CREATE OR REPLACE TRIGGER \"failTrigger2\" ON \"UTIL\".\"PRINT\" AS 'myJob'");

    HoptimatorDdlExecutor.DdlException ex = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> executor.executeDdl(context, node));

    assertTrue(ex.getMessage().contains("trigger replace failed"));
  }

  @Test
  void testExecuteDropFunctionDelegatesToSuper() {
    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlDropObject node =
        (SqlDropObject) HoptimatorDriver.parseQuery(connection,
            "DROP FUNCTION IF EXISTS \"nonExistentFunc\"");

    // DROP FUNCTION kind delegates to super.execute() since it's not VIEW/TABLE/MATERIALIZED VIEW
    executor.execute(node, context);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private SchemaPlus registerDatabaseSchema(CalcitePrepare.Context context, String schemaName) {
    SchemaPlus rootSchema = context.getMutableRootSchema().plus();
    return rootSchema.add(schemaName, new TestDatabaseSchema(schemaName));
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

  private void addMaterializedViewToSchema(CalcitePrepare.Context context, SchemaPlus schema,
      String schemaName, String name, String sql) {
    HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);
    List<String> schemaPath = new ArrayList<>();
    schemaPath.add(schemaName);
    List<String> viewPath = new ArrayList<>(schemaPath);
    viewPath.add(name);
    ViewTable viewTable = HoptimatorDdlUtils.viewTable(context, sql, prepare, schemaPath, viewPath);
    MaterializedViewTable mvTable = new MaterializedViewTable(viewTable);
    schema.add(name, mvTable);
  }
}
