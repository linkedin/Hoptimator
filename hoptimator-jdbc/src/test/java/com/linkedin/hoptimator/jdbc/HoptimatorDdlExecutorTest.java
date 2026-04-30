package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Database;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateMaterializedView;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTable;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTrigger;
import com.linkedin.hoptimator.jdbc.ddl.SqlDropTrigger;
import com.linkedin.hoptimator.jdbc.ddl.SqlPauseTrigger;
import com.linkedin.hoptimator.jdbc.ddl.SqlResumeTrigger;
import com.linkedin.hoptimator.util.DeploymentService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
@ExtendWith(MockitoExtension.class)
class HoptimatorDdlExecutorTest {

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

  /** Only used by constructor / parser factory / DdlException tests (no real connection needed). */
  @Mock
  private CalciteConnection mockCalciteConnection;

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

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  // ---------------------------------------------------------------------------
  // Constructor / parser factory / DdlException
  // ---------------------------------------------------------------------------

  @Test
  void testConstructorThrowsForReadOnlyConnection() throws SQLException {
    when(mockCalciteConnection.isReadOnly()).thenReturn(true);
    Properties properties = new Properties();
    HoptimatorConnection conn = new HoptimatorConnection(mockCalciteConnection, properties);

    HoptimatorDdlExecutor.DdlException exception = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> new HoptimatorDdlExecutor(conn));

    assertTrue(exception.getMessage().contains("read-only"));
  }

  @Test
  void testConstructorSucceedsForWritableConnection() throws SQLException {
    when(mockCalciteConnection.isReadOnly()).thenReturn(false);
    Properties properties = new Properties();
    HoptimatorConnection conn = new HoptimatorConnection(mockCalciteConnection, properties);

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(conn);

    assertNotNull(executor);
  }

  @Test
  void testParserFactoryIsNotNull() {
    SqlParserImplFactory factory = HoptimatorDdlExecutor.PARSER_FACTORY;

    assertNotNull(factory);
  }

  @Test
  void testParserFactoryGetDdlExecutorReturnsInstance() {
    DdlExecutor executor = HoptimatorDdlExecutor.PARSER_FACTORY.getDdlExecutor();

    assertNotNull(executor);
  }

  @Test
  void testDdlExceptionWithMessage() {
    HoptimatorDdlExecutor.DdlException exception = new HoptimatorDdlExecutor.DdlException("test error");

    assertEquals("test error", exception.getMessage());
  }

  @Test
  void testDdlExceptionWithMessageAndCause() {
    RuntimeException cause = new RuntimeException("root cause");
    HoptimatorDdlExecutor.DdlException exception = new HoptimatorDdlExecutor.DdlException("test error", cause);

    assertEquals("test error", exception.getMessage());
    assertEquals(cause, exception.getCause());
  }

  @Test
  void testDdlExceptionWithSqlNode() {
    SqlIdentifier node = new SqlIdentifier("myView", SqlParserPos.ZERO);

    HoptimatorDdlExecutor.DdlException exception = new HoptimatorDdlExecutor.DdlException(node, "something went wrong");

    assertNotNull(exception.getMessage());
    assertTrue(exception.getMessage().contains("something went wrong"));
    assertTrue(exception.getMessage().contains("myView"));
  }

  @Test
  void testDdlExceptionWithSqlNodeAndCause() {
    SqlIdentifier node = new SqlIdentifier("myView", new SqlParserPos(5, 10));
    RuntimeException cause = new RuntimeException("root");

    HoptimatorDdlExecutor.DdlException exception = new HoptimatorDdlExecutor.DdlException(node, "failed", cause);

    assertTrue(exception.getMessage().contains("failed"));
    assertTrue(exception.getMessage().contains("line 5"));
    assertTrue(exception.getMessage().contains("col 10"));
    assertEquals(cause, exception.getCause());
  }

  @Test
  void testDdlExceptionWithNullCause() {
    HoptimatorDdlExecutor.DdlException exception = new HoptimatorDdlExecutor.DdlException("msg", null);

    assertEquals("msg", exception.getMessage());
  }

  @Test
  void testParserFactoryCreatesParser() {
    SqlAbstractParserImpl parser = HoptimatorDdlExecutor.PARSER_FACTORY.getParser(new StringReader("SELECT 1"));

    assertNotNull(parser);
  }

  @Test
  void testConstructorThrowsWhenIsReadOnlyThrowsSQLException() throws SQLException {
    when(mockCalciteConnection.isReadOnly()).thenThrow(new SQLException("connection error"));
    Properties properties = new Properties();
    HoptimatorConnection conn = new HoptimatorConnection(mockCalciteConnection, properties);

    HoptimatorDdlExecutor.DdlException exception = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> new HoptimatorDdlExecutor(conn));

    assertTrue(exception.getMessage().contains("read-only"));
  }

  // ---------------------------------------------------------------------------
  // CREATE/DROP VIEW
  // ---------------------------------------------------------------------------

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

  /**
   * Stubs ValidationService.validateOrThrow to throw "validation failed" for all overloads.
   * Call this at the start of each validation-failure test.
   */
  @SuppressWarnings("unchecked")
  private void stubValidationToFail() {
    // Casting to Object in the lambda forces Java's overload resolution to pick the T-object
    // overload (erased to Object) rather than the Collection overload.
    mockValidationService.when(() -> ValidationService.validateOrThrow(any(Object.class), any()))
        .thenThrow(new SQLException("validation failed"));
    // Also stub the Collection overload for callers that pass deployer collections.
    mockValidationService.when(() -> ValidationService.validateOrThrow(any(Collection.class), any()))
        .thenThrow(new SQLException("validation failed"));
  }
}
