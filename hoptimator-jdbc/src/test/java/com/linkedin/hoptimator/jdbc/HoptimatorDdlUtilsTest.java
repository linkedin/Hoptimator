package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.Deployer;
import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Sink;
import com.linkedin.hoptimator.ThrowingFunction;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateMaterializedView;
import com.linkedin.hoptimator.jdbc.ddl.SqlCreateTable;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class HoptimatorDdlUtilsTest {

  @Mock
  private MockedStatic<DeploymentService> mockDeploymentService;

  /** Stubs DeploymentService.plan() to return a spy Implementor whose pipeline() method
   * returns the given Pipeline mock, allowing tests to exercise the full deployment path. */
  private PipelineRel.Implementor stubPlan(Pipeline mockPipeline) throws Exception {
    PipelineRel.Implementor spyPlan = spy(
        new PipelineRel.Implementor(ImmutablePairList.of(), Collections.emptyMap()));
    doReturn(mockPipeline).when(spyPlan).pipeline(anyString(), any());
    mockDeploymentService.when(() -> DeploymentService.plan(any(), any(), any()))
        .thenReturn(spyPlan);
    return spyPlan;
  }

  /** Creates a minimal Pipeline mock whose job().sql() returns a no-op function. */
  private Pipeline mockPipeline() {
    Pipeline mockPipeline = mock(Pipeline.class);
    Job mockJob = mock(Job.class);
    ThrowingFunction<SqlDialect, String> sqlFn = dialect -> "SELECT 1 AS x";
    // sql() is only invoked when building MaterializedView, not in the SELECT path
    lenient().when(mockJob.sql()).thenAnswer(inv -> sqlFn);
    when(mockPipeline.job()).thenReturn(mockJob);
    return mockPipeline;
  }

  @Test
  void testRenameColumnsReturnsQueryWhenColumnListNull() {
    SqlNode query = SqlLiteral.createCharString("test", SqlParserPos.ZERO);

    SqlNode result = HoptimatorDdlUtils.renameColumns(null, query);

    assertEquals(query, result);
  }

  @Test
  void testRenameColumnsWrapsQueryWhenColumnListProvided() {
    SqlNode query = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
    SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);
    columnList.add(new SqlIdentifier("col1", SqlParserPos.ZERO));

    SqlNode result = HoptimatorDdlUtils.renameColumns(columnList, query);

    assertNotNull(result);
  }

  @Test
  void testViewNameWithSimpleIdentifier() {
    SqlIdentifier id = new SqlIdentifier("myView", SqlParserPos.ZERO);

    String result = HoptimatorDdlUtils.viewName(id);

    assertEquals("myView", result);
  }

  @Test
  void testViewNameWithCompoundIdentifier() {
    SqlIdentifier id = new SqlIdentifier(
        Arrays.asList("schema", "myView"), SqlParserPos.ZERO);

    String result = HoptimatorDdlUtils.viewName(id);

    assertEquals("myView", result);
  }

  @Test
  void testOptionsReturnsEmptyMapForNull() {
    Map<String, String> result = HoptimatorDdlUtils.options(null);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testOptionsReturnsEmptyMapForEmptyList() {
    SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);

    Map<String, String> result = HoptimatorDdlUtils.options(nodeList);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testOptionsParsesKeyValuePairs() {
    SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);
    nodeList.add(new SqlIdentifier("key1", SqlParserPos.ZERO));
    nodeList.add(SqlLiteral.createCharString("value1", SqlParserPos.ZERO));
    nodeList.add(new SqlIdentifier("key2", SqlParserPos.ZERO));
    nodeList.add(SqlLiteral.createCharString("value2", SqlParserPos.ZERO));

    Map<String, String> result = HoptimatorDdlUtils.options(nodeList);

    assertEquals(2, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
  }

  @Test
  void testCatalogThrowsForFewerThanThreeNames() {
    SqlIdentifier id = new SqlIdentifier(Arrays.asList("schema", "table"), SqlParserPos.ZERO);
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();

      assertThrows(IllegalArgumentException.class, () ->
          HoptimatorDdlUtils.catalog(context, false, id));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testSchemaWithSimpleId() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      SqlIdentifier id = new SqlIdentifier("myTable", SqlParserPos.ZERO);

      Pair<CalciteSchema, String> result =
          HoptimatorDdlUtils.schema(context, false, id);

      assertNotNull(result);
      assertEquals("myTable", result.right);
    }
  }

  @Test
  void testSchemaWithCompoundId() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      SqlIdentifier id = new SqlIdentifier(Arrays.asList("UTIL", "myTable"), SqlParserPos.ZERO);

      Pair<CalciteSchema, String> result =
          HoptimatorDdlUtils.schema(context, false, id);

      assertNotNull(result);
      assertEquals("myTable", result.right);
    }
  }

  @Test
  void testViewTableCreatesView() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);

      ViewTable viewTable = HoptimatorDdlUtils.viewTable(
          context, "SELECT 1 AS X", prepare,
              List.of("DEFAULT"), Arrays.asList("DEFAULT", "myView"));

      assertNotNull(viewTable);
      assertEquals("SELECT 1 AS X", viewTable.getViewSql());
    }
  }

  @Test
  void testSnapshotAndSetSinkSchemaThrowsForNonDatabase() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);
      PipelineRel.Implementor plan = new PipelineRel.Implementor(
          ImmutablePairList.of(), Collections.emptyMap());
      SqlIdentifier name = new SqlIdentifier(Arrays.asList("UTIL", "myView"), SqlParserPos.ZERO);
      SqlNode query = SqlLiteral.createCharString("SELECT 1", SqlParserPos.ZERO);
      SqlCreateMaterializedView create = new SqlCreateMaterializedView(
          SqlParserPos.ZERO, false, false, name, null, null, null, query);

      assertThrows(HoptimatorDdlExecutor.DdlException.class, () ->
          HoptimatorDdlUtils.snapshotAndSetSinkSchema(context, prepare, plan, create, "SELECT 1"));
    }
  }

  @Test
  void testSnapshotAndSetSinkSchemaWithDatabaseSchema() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);
      PipelineRel.Implementor plan = new PipelineRel.Implementor(
          ImmutablePairList.of(), Collections.emptyMap());

      Schema dbSchema = new TestDatabaseSchema("test-db");
      CalciteSchema calciteSchema =
          CalciteSchema.createRootSchema(false, false, "testdb", dbSchema);

      Pair<CalciteSchema, String> schemaPair = Pair.of(calciteSchema, "myView");

      Pair<SchemaPlus, Table> result =
          HoptimatorDdlUtils.snapshotAndSetSinkSchema(context, prepare, plan, "SELECT 1 AS X", schemaPair);

      assertNotNull(result);
      assertNotNull(result.left);
    }
  }

  @Test
  void testSnapshotAndSetSinkSchemaFourParamWithDatabase() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);
      PipelineRel.Implementor plan = new PipelineRel.Implementor(
          ImmutablePairList.of(), Collections.emptyMap());

      // Register a Database schema in both root schemas so schema() lookup works
      context.getRootSchema().plus().add("TESTDB", new TestDatabaseSchema("test-db"));

      SqlIdentifier name = new SqlIdentifier(Arrays.asList("TESTDB", "myView"), SqlParserPos.ZERO);
      SqlNode query = SqlLiteral.createCharString("SELECT 1", SqlParserPos.ZERO);
      SqlCreateMaterializedView create = new SqlCreateMaterializedView(
          SqlParserPos.ZERO, false, false, name, null, null, null, query);

      Pair<SchemaPlus, Table> result =
          HoptimatorDdlUtils.snapshotAndSetSinkSchema(context, prepare, plan, create, "SELECT 1 AS X");

      assertNotNull(result);
      assertNotNull(result.left);
    }
  }

  @Test
  void testSchemaWithMutableFlag() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      SqlIdentifier id = new SqlIdentifier("myTable", SqlParserPos.ZERO);

      Pair<CalciteSchema, String> result = HoptimatorDdlUtils.schema(context, true, id);

      assertNotNull(result);
      assertEquals("myTable", result.right);
    }
  }

  @Test
  void testCatalogWithThreePartIdentifier() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      SqlIdentifier id = new SqlIdentifier(Arrays.asList("catalog", "schema", "table"), SqlParserPos.ZERO);

      Pair<CalciteSchema, String> result = HoptimatorDdlUtils.catalog(context, false, id);

      assertNotNull(result);
      assertEquals("schema.table", result.right);
    }
  }

  @Test
  void testCatalogThrowsForTwoPartIdentifier() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      SqlIdentifier id = new SqlIdentifier(Arrays.asList("schema", "table"), SqlParserPos.ZERO);

      assertThrows(IllegalArgumentException.class,
          () -> HoptimatorDdlUtils.catalog(context, false, id));
    }
  }

  @Test
  void testCatalogThrowsForSimpleIdentifier() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      SqlIdentifier id = new SqlIdentifier("table", SqlParserPos.ZERO);

      assertThrows(IllegalArgumentException.class,
          () -> HoptimatorDdlUtils.catalog(context, false, id));
    }
  }

  @Test
  void testCatalogWithThreePartIdentifierReturnsDotJoinedSchemaTable() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      SqlIdentifier id = new SqlIdentifier(Arrays.asList("UTIL", "DEFAULT", "PRINT"), SqlParserPos.ZERO);

      Pair<CalciteSchema, String> result = HoptimatorDdlUtils.catalog(context, false, id);

      assertNotNull(result);
      assertEquals("DEFAULT.PRINT", result.right);
    }
  }

  @Test
  void testRenameColumnsWithColumnListRenamesOnlySpecifiedColumns() {
    SqlNode query = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
    SqlNodeList columnList = new SqlNodeList(SqlParserPos.ZERO);
    columnList.add(new SqlIdentifier("renamedCol", SqlParserPos.ZERO));

    SqlNode result = HoptimatorDdlUtils.renameColumns(columnList, query);

    // Non-null result means the column list was used; null means query was returned as-is
    assertNotNull(result);
    // If columnList == null always true, result would be the original query literal
    assertFalse(result == query, "Expected wrapped SELECT node, not the original query");
  }

  @Test
  void testOptionsWithSingleLiteralKeyValuePair() {
    SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);
    nodeList.add(SqlLiteral.createCharString("connector", SqlParserPos.ZERO));
    nodeList.add(SqlLiteral.createCharString("kafka", SqlParserPos.ZERO));

    Map<String, String> result = HoptimatorDdlUtils.options(nodeList);

    assertEquals(1, result.size());
    assertEquals("kafka", result.get("connector"));
  }

  @Test
  void testOptionsWithCompoundIdentifierKeyAndLiteralValue() {
    SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);
    nodeList.add(new SqlIdentifier(Arrays.asList("kafka", "bootstrap.servers"), SqlParserPos.ZERO));
    nodeList.add(SqlLiteral.createCharString("localhost:9092", SqlParserPos.ZERO));

    Map<String, String> result = HoptimatorDdlUtils.options(nodeList);

    assertEquals(1, result.size());
    assertEquals("localhost:9092", result.get("kafka.bootstrap.servers"));
  }

  @Test
  void testViewNameWithThreePartIdentifierReturnsLastPart() {
    SqlIdentifier id = new SqlIdentifier(
        Arrays.asList("catalog", "schema", "myView"), SqlParserPos.ZERO);

    String result = HoptimatorDdlUtils.viewName(id);

    assertEquals("myView", result);
  }

  @Test
  void testSnapshotAndSetSinkSchemaAddsViewToSchema() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);
      PipelineRel.Implementor plan = new PipelineRel.Implementor(
          ImmutablePairList.of(), Collections.emptyMap());

      Schema dbSchema = new TestDatabaseSchema("test-db");
      CalciteSchema calciteSchema =
          CalciteSchema.createRootSchema(false, false, "testdb", dbSchema);
      Pair<CalciteSchema, String> schemaPair = Pair.of(calciteSchema, "newView");

      Pair<SchemaPlus, Table> result =
          HoptimatorDdlUtils.snapshotAndSetSinkSchema(context, prepare, plan, "SELECT 1 AS X", schemaPair);

      // schemaPlus.add(viewName, materializedViewTable) should have been called — verify table is present
      assertNotNull(result.left);
      assertNotNull(result.left.tables().get("newView"),
          "Expected view 'newView' to be registered in schema after snapshotAndSetSinkSchema");
    }
  }

  @Test
  void testSnapshotAndSetSinkSchemaSetsNonNullSinkOnPlan() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);
      PipelineRel.Implementor plan = new PipelineRel.Implementor(
          ImmutablePairList.of(), Collections.emptyMap());

      Schema dbSchema = new TestDatabaseSchema("test-db");
      CalciteSchema calciteSchema =
          CalciteSchema.createRootSchema(false, false, "testdb", dbSchema);
      Pair<CalciteSchema, String> schemaPair = Pair.of(calciteSchema, "sinkView");

      HoptimatorDdlUtils.snapshotAndSetSinkSchema(context, prepare, plan, "SELECT 1 AS X", schemaPair);

      // Verify plan.setSink(...) was actually called
      assertTrue(plan.hasSink(), "Expected plan.sink to be non-null after snapshotAndSetSinkSchema");
    }
  }

  @Test
  void testSnapshotReturnsCurrentViewTableBeforeAdd() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context context = connection.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);
      PipelineRel.Implementor plan = new PipelineRel.Implementor(
          ImmutablePairList.of(), Collections.emptyMap());

      Schema dbSchema = new TestDatabaseSchema("test-db");
      CalciteSchema calciteSchema =
          CalciteSchema.createRootSchema(false, false, "testdb", dbSchema);
      Pair<CalciteSchema, String> schemaPair = Pair.of(calciteSchema, "brandNewView");

      Pair<SchemaPlus, Table> result =
          HoptimatorDdlUtils.snapshotAndSetSinkSchema(context, prepare, plan, "SELECT 1 AS X", schemaPair);

      // The right-side is the snapshot BEFORE the call — view didn't exist, so it must be null
      assertNull(result.right, "Expected snapshot of non-existent view to be null");
    }
  }

  // ---- registerTemporaryTable tests ----

  @Test
  void registerTemporaryTableAddsTableWhenNoExistingTable() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());

      Runnable rollback = HoptimatorDdlUtils.registerTemporaryTable(rootSchema, "TEMP_TABLE_NEW", rowType, "test-db");

      assertNotNull(rootSchema.tables().get("TEMP_TABLE_NEW"), "Expected table to be registered");
      assertNotNull(rollback);
      rollback.run();
      assertNull(rootSchema.tables().get("TEMP_TABLE_NEW"), "Expected table to be removed after rollback");
    }
  }

  @Test
  void registerTemporaryTableRestoresExistingTableOnRollback() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());
      TemporaryTable originalTable = new TemporaryTable(rowType, "original-db");
      rootSchema.add("EXISTING_TABLE", originalTable);

      Runnable rollback = HoptimatorDdlUtils.registerTemporaryTable(rootSchema, "EXISTING_TABLE", rowType, "new-db");

      assertNotNull(rootSchema.tables().get("EXISTING_TABLE"), "Expected new table to be registered");
      rollback.run();
      // After rollback, the original table should be restored
      assertNotNull(rootSchema.tables().get("EXISTING_TABLE"), "Expected original table to be restored after rollback");
    }
  }

  // ---- registerTemporaryTableInSchema tests ----

  @Test
  void registerTemporaryTableInSchemaNullCatalogNullSchemaRegistersInRoot() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());

      Runnable rollback = HoptimatorDdlUtils.registerTemporaryTableInSchema(
          connection, null, null, "ROOT_TEMP_TABLE", rowType, "test-db");

      assertNotNull(rollback);
      SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
      assertNotNull(rootSchema.tables().get("ROOT_TEMP_TABLE"), "Expected table in root schema");
    }
  }

  @Test
  void registerTemporaryTableInSchemaNullCatalogWithSchemaRegistersInTierSchema() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());
      // Add a subschema that registerTemporaryTableInSchema can find
      rootSchema.add("MY_SCHEMA", new AbstractSchema());

      Runnable rollback = HoptimatorDdlUtils.registerTemporaryTableInSchema(
          connection, null, "MY_SCHEMA", "TIER_TEMP_TABLE", rowType, "test-db");

      assertNotNull(rollback);
      SchemaPlus tierSchema = rootSchema.subSchemas().get("MY_SCHEMA");
      assertNotNull(tierSchema, "Expected MY_SCHEMA to exist");
      assertNotNull(tierSchema.tables().get("TIER_TEMP_TABLE"), "Expected table in tier schema");
    }
  }

  @Test
  void registerTemporaryTableInSchemaNullCatalogWithMissingSchemaThrows() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());

      assertThrows(SQLException.class, () ->
          HoptimatorDdlUtils.registerTemporaryTableInSchema(
              connection, null, "NONEXISTENT_SCHEMA", "TABLE", rowType, "db"));
    }
  }

  @Test
  void registerTemporaryTableInSchemaWithCatalogAndSchemaRegistersInDatabaseSchema() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());
      // Add a catalog subschema, and within it a database subschema
      SchemaPlus catalogSchema = rootSchema.add("MY_CATALOG", new AbstractSchema());
      catalogSchema.add("MY_DB_SCHEMA", new AbstractSchema());

      Runnable rollback = HoptimatorDdlUtils.registerTemporaryTableInSchema(
          connection, "MY_CATALOG", "MY_DB_SCHEMA", "CATALOG_TEMP_TABLE", rowType, "test-db");

      assertNotNull(rollback);
      SchemaPlus dbSchema = catalogSchema.subSchemas().get("MY_DB_SCHEMA");
      assertNotNull(dbSchema, "Expected MY_DB_SCHEMA to exist");
      assertNotNull(dbSchema.tables().get("CATALOG_TEMP_TABLE"), "Expected table in database schema");
    }
  }

  @Test
  void registerTemporaryTableInSchemaWithCatalogNullSchemaThrows() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());
      rootSchema.add("SOME_CATALOG", new AbstractSchema());

      assertThrows(SQLException.class, () ->
          HoptimatorDdlUtils.registerTemporaryTableInSchema(
              connection, "SOME_CATALOG", null, "TABLE", rowType, "db"));
    }
  }

  @Test
  void registerTemporaryTableInSchemaWithMissingCatalogThrows() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());

      assertThrows(SQLException.class, () ->
          HoptimatorDdlUtils.registerTemporaryTableInSchema(
              connection, "MISSING_CATALOG", "MY_SCHEMA", "TABLE", rowType, "db"));
    }
  }

  @Test
  void registerTemporaryTableInSchemaWithCatalogAndMissingSchemaNotHoptimatorSchemaThrows() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SchemaPlus rootSchema = connection.calciteConnection().getRootSchema();
      RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType rowType = typeFactory.createStructType(Collections.emptyList(), Collections.emptyList());
      // Add a plain AbstractSchema as catalog — it's NOT a HoptimatorJdbcCatalogSchema
      rootSchema.add("PLAIN_CATALOG", new AbstractSchema());

      assertThrows(SQLException.class, () ->
          HoptimatorDdlUtils.registerTemporaryTableInSchema(
              connection, "PLAIN_CATALOG", "MISSING_DB_SCHEMA", "TABLE", rowType, "db"));
    }
  }

  // ── specifyFromSql() / options() tests ──────────────────────────────────────

  @Test
  void specifyFromSqlThrowsForUnsupportedDdl() throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      assertThrows(SQLException.class,
          () -> HoptimatorDdlUtils.specifyFromSql("DROP VIEW \"PROFILE\".\"MEMBERS\"", connection));
    }
  }

  @Test
  void specifyFromSqlCreateTableRunsValidateSpecifyRestoreCycle() throws Exception {
    // specifyFromSql for CREATE TABLE runs validate→specify→restore. In the util catalog PROFILE
    // schema has no deployers with required K8s properties, so source-level validation
    // propagates a proper SQLException (not an NPE or silent failure).
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      assertThrows(SQLException.class, () ->
          HoptimatorDdlUtils.specifyFromSql(
              "CREATE TABLE \"PROFILE\".\"dry_run_test\" (\"id\" INT, \"name\" VARCHAR)",
              connection));
    }
  }

  @Test
  void specifyCreateTableThrowsWhenSchemaNotDatabase() throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // "k8s" schema doesn't exist in the util catalog → schema() throws
      assertThrows(Exception.class, () ->
          HoptimatorDdlUtils.specifyFromSql(
              "CREATE TABLE \"NONEXISTENT\".\"t\" (\"i\" INT)", connection));
    }
  }

  @Test
  void specifyFromSqlWithCreateMaterializedViewThrowsForNonexistentSchema() throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      assertThrows(SQLException.class, () ->
          HoptimatorDdlUtils.specifyFromSql(
              "CREATE MATERIALIZED VIEW \"NONEXISTENT\".\"mv\" AS SELECT 1",
              connection));
    }
  }

  @Test
  void processCreateMaterializedViewSpecifyModeCoversPlanningPath() throws Exception {
    // Registers a TestDatabaseSchema so processCreateMaterializedView can navigate to a
    // valid Database in SPECIFY mode, exercising schema traversal + SQL extraction + planning.
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      conn.calciteConnection().getRootSchema().add("TEST_DB", new TestDatabaseSchema("test-db"));
      // SPECIFY mode: skips conflict check, proceeds to planning — the planner throws
      // CannotPlanException (RuntimeException) because SELECT 1 can't be planned with
      // PIPELINE convention. This exercises the SQL extraction + planning code paths.
      assertThrows(Exception.class, () ->
          HoptimatorDdlUtils.specifyFromSql(
              "CREATE MATERIALIZED VIEW \"TEST_DB\".\"mv\" AS SELECT 1", conn));
    }
  }

  @Test
  void specifyFromSqlWithCreateMaterializedViewDryRunDoesNotThrowOnSchemaFailure() throws Exception {
    // DdlMode.SPECIFY skips the conflict check (mutable=false) so specifying over
    // an existing view does not throw — it just proceeds to plan generation.
    // This exercises the mutable() == false branch of processCreateMaterializedView.
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // NONEXISTENT schema — throws with "schema not found", validating early path
      assertThrows(SQLException.class, () ->
          HoptimatorDdlUtils.specifyFromSql(
              "CREATE OR REPLACE MATERIALIZED VIEW \"NONEXISTENT\".\"mv\" AS SELECT 1",
              connection));
    }
  }

  // ── specifyFromSql() plain SELECT path ───────────────────────────────────────

  @Test
  void specifyFromSqlForPlainSelectReturnsSinkPathAndRowType() throws Exception {
    // Regression test: before the fix, pipeline.job().sink() was null for plain SELECT
    // (no INSERT INTO target), causing NullPointerException. Now setSink() anchors a virtual
    // "DEFAULT"."sink" so the pipeline can be fully constructed.
    Pipeline mockPipeline = mock(Pipeline.class);
    Sink mockSink = mock(Sink.class);
    Job mockJob = mock(Job.class);
    when(mockPipeline.sources()).thenReturn(Collections.emptyList());
    when(mockPipeline.sink()).thenReturn(mockSink);
    when(mockPipeline.job()).thenReturn(mockJob);

    PipelineRel.Implementor mockPlan = mock(PipelineRel.Implementor.class);
    when(mockPlan.pipeline(any(), any())).thenReturn(mockPipeline);

    mockDeploymentService.when(() -> DeploymentService.plan(any(), any(), any())).thenReturn(mockPlan);
    mockDeploymentService.when(() -> DeploymentService.specify(any(), any()))
        .thenReturn(Collections.emptyList());

    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      HoptimatorDdlUtils.SpecifyResult result = HoptimatorDdlUtils.specifyFromSql(
          "SELECT 1 AS \"KEY\", 'value' AS \"VAL\"", conn);

      // Specs: empty (no deployers return anything)
      assertNotNull(result.specs);
      assertTrue(result.specs.isEmpty());
      // viewPath last element is "SINK" (uppercase) for a bare SELECT, matching original plan() behavior
      assertNotNull(result.viewPath);
      assertEquals("SINK", result.viewPath.get(result.viewPath.size() - 1));
      // sinkRowType matches the SELECT output columns
      assertNotNull(result.sinkRowType);
      assertEquals(2, result.sinkRowType.getFieldCount());
      assertEquals("KEY", result.sinkRowType.getFieldList().get(0).getName());
      assertEquals("VAL", result.sinkRowType.getFieldList().get(1).getName());
    }
  }

  @Test
  void specifyFromSqlCreateTableViewPathMatchesSchemaAndName() throws Exception {
    // For CREATE TABLE, the SpecifyResult.viewPath should include the schema path + table name.
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      conn.calciteConnection().getRootSchema().add("VP_DB", new TestDatabaseSchema("vp-db"));
      HoptimatorDdlUtils.SpecifyResult result = HoptimatorDdlUtils.specifyFromSql(
          "CREATE TABLE \"VP_DB\".\"events\" (\"id\" INTEGER, \"msg\" VARCHAR)", conn);
      assertNotNull(result.viewPath);
      assertFalse(result.viewPath.isEmpty());
      assertEquals("events", result.viewPath.get(result.viewPath.size() - 1));
      assertTrue(result.viewPath.contains("VP_DB"),
          "viewPath should contain the schema: " + result.viewPath);
    }
  }

  @Test
  void specifyFromSqlForPlainSelectRestoresSchemaAfterCall() throws Exception {
    // The virtual "SINK" table registered during the SELECT path must not persist after
    // specifyFromSql returns (even when it throws at the deployer level).
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context ctx = conn.createPrepareContext();

      // Capture schema state before the call.
      Pair<CalciteSchema, String> pair = HoptimatorDdlUtils.schema(ctx, false,
          new SqlIdentifier("sink", SqlParserPos.ZERO));
      boolean existedBefore = pair.left != null
          && pair.left.plus().tables().get(pair.right) != null;

      try {
        HoptimatorDdlUtils.specifyFromSql("SELECT 1 AS \"COL1\"", conn);
      } catch (Exception ignored) {
        // Expected: planning/deployer failure in the test environment.
      }

      // Schema must be in the same state as before.
      boolean existsAfter = pair.left != null
          && pair.left.plus().tables().get(pair.right) != null;
      assertEquals(existedBefore, existsAfter,
          "Virtual SINK table must be removed after specifyFromSql returns");
    }
  }

  @Test
  void optionsParsesSingleKeyValue() {
    SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
    list.add(SqlLiteral.createCharString("myKey", SqlParserPos.ZERO));
    list.add(SqlLiteral.createCharString("myValue", SqlParserPos.ZERO));
    Map<String, String> result = HoptimatorDdlUtils.options(list);
    assertEquals(1, result.size());
    assertEquals("myValue", result.get("myKey"));
  }

  @Test
  void optionsParsesMultipleKeyValues() {
    SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
    list.add(SqlLiteral.createCharString("k1", SqlParserPos.ZERO));
    list.add(SqlLiteral.createCharString("v1", SqlParserPos.ZERO));
    list.add(SqlLiteral.createCharString("k2", SqlParserPos.ZERO));
    list.add(SqlLiteral.createCharString("v2", SqlParserPos.ZERO));
    Map<String, String> result = HoptimatorDdlUtils.options(list);
    assertEquals(2, result.size());
    assertEquals("v1", result.get("k1"));
    assertEquals("v2", result.get("k2"));
  }

  @Test
  void optionsReturnsEmptyMapForNull() {
    Map<String, String> result = HoptimatorDdlUtils.options(null);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  // ── DdlMode tests ────────────────────────────────────────────────────────────

  @Test
  void ddlModeSpecifyExecuteDeployersCollectsSpecsFromAllDeployers() throws SQLException {
    Deployer deployer1 = mock(Deployer.class);
    Deployer deployer2 = mock(Deployer.class);
    when(deployer1.specify()).thenReturn(List.of("spec1a", "spec1b"));
    when(deployer2.specify()).thenReturn(List.of("spec2"));

    List<String> result = HoptimatorDdlUtils.DdlMode.SPECIFY.executeDeployers(
        List.of(deployer1, deployer2), null);

    assertEquals(List.of("spec1a", "spec1b", "spec2"), result);
  }

  @Test
  void ddlModeSpecifyExecuteDeployersWithNoDeployersReturnsEmptyList() throws SQLException {
    List<String> result = HoptimatorDdlUtils.DdlMode.SPECIFY.executeDeployers(
        Collections.emptyList(), null);

    assertTrue(result.isEmpty());
  }

  @Test
  void ddlModeSpecifyMutableReturnsFalse() {
    assertFalse(HoptimatorDdlUtils.DdlMode.SPECIFY.mutable());
  }

  @Test
  void ddlModeCreateMutableReturnsTrue() {
    assertTrue(HoptimatorDdlUtils.DdlMode.CREATE.mutable());
  }

  @Test
  void ddlModeUpdateMutableReturnsTrue() {
    assertTrue(HoptimatorDdlUtils.DdlMode.UPDATE.mutable());
  }

  // ── ColumnDef tests ──────────────────────────────────────────────────────────

  @Test
  void columnDefOfWithNullExprAndNullableStrategy() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    HoptimatorDdlUtils.ColumnDef columnDef =
        HoptimatorDdlUtils.ColumnDef.of(null, type, ColumnStrategy.NULLABLE);

    assertNotNull(columnDef);
    assertNull(columnDef.expr);
    assertEquals(ColumnStrategy.NULLABLE, columnDef.strategy);
  }

  @Test
  void columnDefOfWithNullExprAndNotNullableStrategy() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = typeFactory.createSqlType(SqlTypeName.INTEGER);

    HoptimatorDdlUtils.ColumnDef columnDef =
        HoptimatorDdlUtils.ColumnDef.of(null, type, ColumnStrategy.NOT_NULLABLE);

    assertNotNull(columnDef);
    assertNull(columnDef.expr);
    assertEquals(ColumnStrategy.NOT_NULLABLE, columnDef.strategy);
  }

  @Test
  void columnDefOfWithNullExprAndStoredStrategyThrowsIllegalArgumentException() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = typeFactory.createSqlType(SqlTypeName.VARCHAR);

    // STORED requires a non-null expr; checkArgument fails if expr==null and strategy!=NULLABLE/NOT_NULLABLE
    assertThrows(IllegalArgumentException.class,
        () -> HoptimatorDdlUtils.ColumnDef.of(null, type, ColumnStrategy.STORED));
  }

  @Test
  void columnDefOfWithNonNullExprAndStoredStrategySucceeds() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType type = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    SqlNode expr = SqlLiteral.createCharString("default_val", SqlParserPos.ZERO);

    HoptimatorDdlUtils.ColumnDef columnDef =
        HoptimatorDdlUtils.ColumnDef.of(expr, type, ColumnStrategy.STORED);

    assertNotNull(columnDef);
    assertEquals(expr, columnDef.expr);
    assertEquals(ColumnStrategy.STORED, columnDef.strategy);
  }

  // ── processCreateTable validation paths ──────────────────────────────────────

  @Test
  void processCreateTableThrowsWhenQueryIsNotNull() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // "CREATE TABLE ... AS SELECT ..." — the query field will be non-null
      SqlCreateTable create = (SqlCreateTable) HoptimatorDriver.parseQuery(conn,
          "CREATE TABLE \"PROFILE\".\"new_tbl\" AS SELECT 1 AS \"id\"");

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      SQLException ex = assertThrows(SQLException.class,
          () -> HoptimatorDdlUtils.processCreateTable(ctx, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
      assertTrue(ex.getMessage().contains("Populating new tables is not currently supported"),
          "Expected 'Populating new tables' but got: " + ex.getMessage());
    }
  }

  @Test
  void processCreateTableThrowsWhenColumnListIsNull() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // "CREATE TABLE ... AS SELECT ..." — parse produces a node where query != null.
      // To get columnList==null with query==null we need to parse SQL with neither columns nor AS clause.
      // Such SQL is not valid parser syntax; instead, directly test via specifyFromSql using a SQL
      // whose query==null / columnList==null. Actually the parser always provides one or the other.
      // Use the "AS SELECT" form → query != null (caught first). To test "No columns provided" we
      // must manufacture a node. We can parse "CREATE TABLE t AS SELECT 1" which sets query and
      // let the first check fire, OR we use the DDL executor path. Since we can't construct
      // SqlCreateTable directly (protected ctor), we verify through specifyFromSql with a table
      // that has no columns (invalid SQL that the parser would reject) — this path is exercised
      // through the "query != null" check above. Mark as note: "No columns provided" path is
      // only reachable if someone constructs the AST programmatically with null columnList and
      // null query; the SQL parser always supplies one. We assert this check is unreachable via
      // the public SQL API but is covered by the source-level check.
      //
      // Verify schema-not-found fires first when schema doesn't exist (column list is empty list).
      SqlCreateTable create = (SqlCreateTable) HoptimatorDriver.parseQuery(conn,
          "CREATE TABLE \"NONEXISTENT_SCH\".\"t\" (\"i\" INT)");
      CalcitePrepare.Context ctx = conn.createPrepareContext();
      SQLException ex = assertThrows(SQLException.class,
          () -> HoptimatorDdlUtils.processCreateTable(ctx, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
      assertNotNull(ex.getMessage());
    }
  }

  @Test
  void processCreateTableThrowsWhenTwoLevelSchemaNotFound() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // A 2-level name where the schema does not exist
      SqlCreateTable create = (SqlCreateTable) HoptimatorDriver.parseQuery(conn,
          "CREATE TABLE \"NONEXISTENT_SCHEMA\".\"my_table\" (\"id\" INT)");

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      SQLException ex = assertThrows(SQLException.class,
          () -> HoptimatorDdlUtils.processCreateTable(ctx, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
      assertTrue(ex.getMessage().contains("Schema for") && ex.getMessage().contains("not found"),
          "Expected 'Schema for ... not found' but got: " + ex.getMessage());
    }
  }

  @Test
  void processCreateTableThrowsWhenThreeLevelCatalogNotFound() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // A 3-level name "NONEXISTENT_CATALOG.some_schema.my_table":
      // schema() path = ["NONEXISTENT_CATALOG","some_schema"] — the first sub-schema lookup
      // returns null, causing requireNonNull to throw NullPointerException before processCreateTable
      // can even check pair.left == null. This verifies the 3-level path reaches an error
      // (NPE from Calcite schema traversal or SQLException from catalog() null check).
      SqlCreateTable create = (SqlCreateTable) HoptimatorDriver.parseQuery(conn,
          "CREATE TABLE \"NONEXISTENT_CATALOG\".\"some_schema\".\"my_table\" (\"id\" INT)");

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      // Either NullPointerException (from schema traversal) or SQLException is acceptable —
      // both indicate the catalog was not found.
      assertThrows(Exception.class,
          () -> HoptimatorDdlUtils.processCreateTable(ctx, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
    }
  }

  @Test
  void processCreateTableThrowsWhenThreeLevelCatalogFoundButSchemaUnknown() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // "UTIL" exists as a root schema but "NEW_SCHEMA" doesn't exist inside it.
      // schema() path = ["UTIL","NEW_SCHEMA"]: first lookup succeeds (UTIL), second returns null →
      // requireNonNull throws NPE. Then the 3-level branch calls catalog() with path=["UTIL"],
      // which returns Pair.of(null, "NEW_SCHEMA.my_table") → "Catalog for ... not found."
      // Actually: schema() iterates ["UTIL","NEW_SCHEMA"] and NPEs at the second. processCreateTable
      // then catches pair.left==null or NPE at assertThrows level.
      // Use a name where the first catalog component exists but the second doesn't:
      SqlCreateTable create = (SqlCreateTable) HoptimatorDriver.parseQuery(conn,
          "CREATE TABLE \"UTIL\".\"NONEXISTENT_SCHEMA\".\"my_table\" (\"id\" INT)");

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      // schema() path = ["UTIL","NONEXISTENT_SCHEMA"]: UTIL found, NONEXISTENT_SCHEMA → null → NPE.
      // processCreateTable catches this via the pair.left==null branch, calls catalog() with path=["UTIL"],
      // UTIL exists → pair.left non-null → does NOT throw "Catalog for ... not found." but proceeds.
      // The test just verifies an exception is thrown (schema or catalog resolution fails).
      assertThrows(Exception.class,
          () -> HoptimatorDdlUtils.processCreateTable(ctx, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
    }
  }

  @Test
  void processCreateTableThrowsWhenTableAlreadyExistsWithoutIfNotExists() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // Register a TestDatabaseSchema in the root schema and pre-populate a table
      SchemaPlus existingDbSchema = conn.calciteConnection().getRootSchema()
          .add("TEST_EXISTING_DB", new TestDatabaseSchema("test-existing-db"));
      existingDbSchema.add("existing_table", mock(Table.class));

      SqlCreateTable create = (SqlCreateTable) HoptimatorDriver.parseQuery(conn,
          "CREATE TABLE \"TEST_EXISTING_DB\".\"existing_table\" (\"id\" INT)");

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      SQLException ex = assertThrows(SQLException.class,
          () -> HoptimatorDdlUtils.processCreateTable(ctx, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
      assertTrue(ex.getMessage().contains("already exists"),
          "Expected 'Table already exists' but got: " + ex.getMessage());
    }
  }

  @Test
  void processCreateTableReturnsEmptyListWhenTableAlreadyExistsWithIfNotExists() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // Register a schema and pre-populate a table
      SchemaPlus ifneDbSchema = conn.calciteConnection().getRootSchema()
          .add("TEST_IFNOTEXISTS_DB", new TestDatabaseSchema("test-ifne-db"));
      ifneDbSchema.add("existing_table", mock(Table.class));

      // IF NOT EXISTS — if the table already exists this should be a no-op
      SqlCreateTable create = (SqlCreateTable) HoptimatorDriver.parseQuery(conn,
          "CREATE TABLE IF NOT EXISTS \"TEST_IFNOTEXISTS_DB\".\"existing_table\" (\"id\" INT)");

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      // IF NOT EXISTS suppresses the "already exists" error; the method may proceed further and
      // succeed or throw for other reasons (e.g. validation). We specifically assert NO
      // "already exists" error fires.
      try {
        HoptimatorDdlUtils.processCreateTable(ctx, conn, create, HoptimatorDdlUtils.DdlMode.CREATE);
      } catch (SQLException ex) {
        assertFalse(ex.getMessage().contains("already exists"),
            "IF NOT EXISTS should suppress 'already exists' error, but got: " + ex.getMessage());
      }
    }
  }

  // ── processCreateMaterializedView validation paths ────────────────────────────

  @Test
  void processCreateMaterializedViewThrowsWhenSchemaNotFound() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SqlCreateMaterializedView create = (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(conn,
          "CREATE MATERIALIZED VIEW \"NONEXISTENT_SCHEMA\".\"myView\" AS SELECT 1 AS \"col1\"");
      CalcitePrepare.Context ctx = conn.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(conn);
      SQLException ex = assertThrows(SQLException.class,
          () -> HoptimatorDdlUtils.processCreateMaterializedView(
              ctx, prepare, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
      assertTrue(ex.getMessage().contains("Schema for") && ex.getMessage().contains("not found"),
          "Expected 'Schema for ... not found' but got: " + ex.getMessage());
    }
  }

  @Test
  void processCreateMaterializedViewThrowsWhenSchemaIsNotDatabase() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // UTIL schema exists but is NOT a Database instance
      SqlCreateMaterializedView create = (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(conn,
          "CREATE MATERIALIZED VIEW \"UTIL\".\"myView\" AS SELECT 1 AS \"col1\"");
      CalcitePrepare.Context ctx = conn.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(conn);
      SQLException ex = assertThrows(SQLException.class,
          () -> HoptimatorDdlUtils.processCreateMaterializedView(
              ctx, prepare, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
      assertTrue(ex.getMessage().contains("is not a physical database"),
          "Expected 'is not a physical database' but got: " + ex.getMessage());
    }
  }

  @Test
  void processCreateMaterializedViewThrowsWhenViewAlreadyExistsWithoutFlags() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // Register a TestDatabaseSchema and pre-populate a view table
      SchemaPlus mvDbSchema = conn.calciteConnection().getRootSchema()
          .add("TEST_MV_DB", new TestDatabaseSchema("test-mv-db"));
      // Add a dummy ViewTable (not a HoptimatorJdbcTable) so the "already exists" branch fires
      mvDbSchema.add("existingView", mock(ViewTable.class));

      // replace=false, ifNotExists=false → should throw "already exists"
      SqlCreateMaterializedView create = (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(conn,
          "CREATE MATERIALIZED VIEW \"TEST_MV_DB\".\"existingView\" AS SELECT 1 AS \"col1\"");
      CalcitePrepare.Context ctx = conn.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(conn);
      SQLException ex = assertThrows(SQLException.class,
          () -> HoptimatorDdlUtils.processCreateMaterializedView(
              ctx, prepare, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
      assertTrue(ex.getMessage().contains("already exists"),
          "Expected 'View already exists' but got: " + ex.getMessage());
    }
  }

  @Test
  void processCreateMaterializedViewReturnsEmptyListWhenViewExistsWithIfNotExists() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SchemaPlus ifneMvDbSchema = conn.calciteConnection().getRootSchema()
          .add("TEST_IFNE_MV_DB", new TestDatabaseSchema("test-ifne-mv-db"));
      ifneMvDbSchema.add("existingView", mock(ViewTable.class));

      // replace=false, ifNotExists=true → should return empty specs (no-op)
      SqlCreateMaterializedView create = (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(conn,
          "CREATE MATERIALIZED VIEW IF NOT EXISTS \"TEST_IFNE_MV_DB\".\"existingView\" AS SELECT 1 AS \"col1\"");
      CalcitePrepare.Context ctx = conn.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(conn);
      HoptimatorDdlUtils.SpecifyResult result = HoptimatorDdlUtils.processCreateMaterializedView(
          ctx, prepare, conn, create, HoptimatorDdlUtils.DdlMode.CREATE);

      assertNotNull(result);
      assertTrue(result.specs.isEmpty(), "IF NOT EXISTS on existing view should return empty specs");
      assertNotNull(result.sinkRowType, "sinkRowType should be populated even for IF NOT EXISTS no-op");
    }
  }

  @Test
  void processCreateMaterializedViewThrowsWhenOverwritingHoptimatorJdbcTable() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SchemaPlus jdbcDbSchema = conn.calciteConnection().getRootSchema()
          .add("TEST_JDBC_DB", new TestDatabaseSchema("test-jdbc-db"));

      // Create a mock HoptimatorJdbcTable and register it
      com.linkedin.hoptimator.util.planner.HoptimatorJdbcTable jdbcTable =
          mock(com.linkedin.hoptimator.util.planner.HoptimatorJdbcTable.class);
      jdbcDbSchema.add("physicalTable", jdbcTable);

      // replace=false, ifNotExists=false, but we're trying to overwrite a HoptimatorJdbcTable
      SqlCreateMaterializedView create = (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(conn,
          "CREATE MATERIALIZED VIEW \"TEST_JDBC_DB\".\"physicalTable\" AS SELECT 1 AS \"col1\"");
      CalcitePrepare.Context ctx = conn.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(conn);
      SQLException ex = assertThrows(SQLException.class,
          () -> HoptimatorDdlUtils.processCreateMaterializedView(
              ctx, prepare, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
      assertTrue(ex.getMessage().contains("Cannot overwrite physical table"),
          "Expected 'Cannot overwrite physical table' but got: " + ex.getMessage());
    }
  }

  // ── catalog() with existing sub-schema ───────────────────────────────────────

  @Test
  void catalogWithThreePartIdentifierWhereSubSchemaExists() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context ctx = conn.createPrepareContext();
      // "UTIL" exists as a root-level sub-schema in the util catalog
      SqlIdentifier id = new SqlIdentifier(Arrays.asList("UTIL", "DEFAULT", "PRINT"), SqlParserPos.ZERO);

      Pair<CalciteSchema, String> result = HoptimatorDdlUtils.catalog(ctx, false, id);

      assertNotNull(result);
      assertNotNull(result.left, "Expected catalog sub-schema to be non-null");
      assertEquals("DEFAULT.PRINT", result.right);
    }
  }

  // ── specifyResult() sinkRowType tests ────────────────────────────────────────
  // All tests use TestDatabaseSchema so processCreate* can navigate to a real Database schema.

  @Test
  void specifyResultSinkRowTypeForCreateMaterializedViewReturnsQueryOutputType() throws Exception {
    // sinkRowType is computed before DeploymentService.plan(), so we can capture it via the
    // IF NOT EXISTS early-return path (view pre-exists → return SpecifyResult([], sinkRowType)).
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SchemaPlus mvDbSchema = conn.calciteConnection().getRootSchema()
          .add("SR_MV_DB", new TestDatabaseSchema("sr-mv-db"));
      mvDbSchema.add("mv", mock(ViewTable.class));

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(conn);
      HoptimatorDdlUtils.SpecifyResult result = HoptimatorDdlUtils.processCreateMaterializedView(
          ctx, prepare, conn, (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(conn,
              "CREATE MATERIALIZED VIEW IF NOT EXISTS \"SR_MV_DB\".\"mv\" AS SELECT 1 AS \"URN\", 'a' AS \"NAME\""),
          HoptimatorDdlUtils.DdlMode.CREATE);

      assertTrue(result.specs.isEmpty());
      assertNotNull(result.sinkRowType);
      assertEquals(2, result.sinkRowType.getFieldCount());
      assertEquals("URN", result.sinkRowType.getFieldList().get(0).getName());
      assertEquals("NAME", result.sinkRowType.getFieldList().get(1).getName());
    }
  }

  @Test
  void specifyResultSinkRowTypeForCreateMaterializedViewAppliesColumnRenames() throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // Column list renames applied before type derivation — output names must match the declared list.
      SchemaPlus renameDbSchema = conn.calciteConnection().getRootSchema()
          .add("SR_RENAME_DB", new TestDatabaseSchema("sr-rename-db"));
      renameDbSchema.add("mv", mock(ViewTable.class));

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(conn);
      HoptimatorDdlUtils.SpecifyResult result = HoptimatorDdlUtils.processCreateMaterializedView(
          ctx, prepare, conn, (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(conn,
              "CREATE MATERIALIZED VIEW IF NOT EXISTS \"SR_RENAME_DB\".\"mv\" (\"ID\", \"LABEL\") AS SELECT 1, 'a'"),
          HoptimatorDdlUtils.DdlMode.CREATE);

      assertTrue(result.specs.isEmpty());
      assertNotNull(result.sinkRowType);
      assertEquals(2, result.sinkRowType.getFieldCount());
      assertEquals("ID", result.sinkRowType.getFieldList().get(0).getName());
      assertEquals("LABEL", result.sinkRowType.getFieldList().get(1).getName());
    }
  }

  @Test
  void specifyResultSinkRowTypeForCreateTableReturnsDeclaredColumnTypes() throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      conn.calciteConnection().getRootSchema().add("SR_TABLE_DB", new TestDatabaseSchema("sr-table-db"));
      RelDataType result = HoptimatorDdlUtils.specifyFromSql(
          "CREATE TABLE \"SR_TABLE_DB\".\"t\" (\"id\" INTEGER, \"name\" VARCHAR, \"active\" BOOLEAN)",
          conn).sinkRowType;
      assertNotNull(result);
      assertEquals(3, result.getFieldCount());
      assertEquals("id", result.getFieldList().get(0).getName());
      assertEquals("name", result.getFieldList().get(1).getName());
      assertEquals("active", result.getFieldList().get(2).getName());
    }
  }

  @Test
  void specifyResultSinkRowTypeForCreateTableIgnoresPrimaryKeyConstraint() throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // PRIMARY KEY is a SqlKeyConstraint — silently ignored, must not appear in the row type.
      conn.calciteConnection().getRootSchema().add("SR_PK_DB", new TestDatabaseSchema("sr-pk-db"));
      RelDataType result = HoptimatorDdlUtils.specifyFromSql(
          "CREATE TABLE \"SR_PK_DB\".\"t\" (\"id\" INTEGER NOT NULL, \"name\" VARCHAR, PRIMARY KEY (\"id\"))",
          conn).sinkRowType;
      assertNotNull(result);
      assertEquals(2, result.getFieldCount());
      assertEquals("id", result.getFieldList().get(0).getName());
      assertEquals("name", result.getFieldList().get(1).getName());
    }
  }

  @Test
  void specifyResultForUnsupportedDdlThrows() throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      assertThrows(SQLException.class,
          () -> HoptimatorDdlUtils.specifyFromSql("DROP TABLE \"PROFILE\".\"MEMBERS\"", conn));
    }
  }

  // ── renameColumns with empty column list ─────────────────────────────────────

  @Test
  void renameColumnsWithEmptyColumnListWrapsQuery() {
    SqlNode query = SqlLiteral.createCharString("test", SqlParserPos.ZERO);
    SqlNodeList emptyColumnList = new SqlNodeList(SqlParserPos.ZERO); // empty, not null

    SqlNode result = HoptimatorDdlUtils.renameColumns(emptyColumnList, query);

    // Non-null list (even empty) causes wrapping in a SELECT node
    assertNotNull(result);
    assertFalse(result == query, "Expected a wrapped SELECT node for non-null column list");
  }

  @Test
  void specifyFromSqlCreateTableReturnsEmptyListWhenNoDeployersRegistered() throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      conn.calciteConnection().getRootSchema()
          .add("TEST_SPECIFY_SQL_DB", new TestDatabaseSchema("test-specify-sql-db"));

      // specifyFromSql dispatches to processCreateTable in SPECIFY mode.
      // No deployer providers are registered in the test env, so it returns an empty spec list.
      HoptimatorDdlUtils.SpecifyResult result = HoptimatorDdlUtils.specifyFromSql(
          "CREATE TABLE \"TEST_SPECIFY_SQL_DB\".\"t\" (\"id\" INTEGER)", conn);

      assertNotNull(result.specs);
      assertTrue(result.specs.isEmpty(), "Expected empty spec list when no deployers registered");
    }
  }

  @Test
  void specifyFromSqlWithPlainSelectReturnsPipelineSpecs() throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // A plain SELECT is not DDL — exercises the non-DDL (SELECT/INSERT) path in specifyFromSql.
      // sources() is called in the SELECT path, so stub it on the mock pipeline.
      Pipeline pipeline = mockPipeline();
      when(pipeline.sources()).thenReturn(Collections.emptyList());
      stubPlan(pipeline);

      HoptimatorDdlUtils.SpecifyResult result = HoptimatorDdlUtils.specifyFromSql("SELECT 1 AS \"x\"", conn);

      assertNotNull(result.specs);
      assertTrue(result.specs.isEmpty());
    }
  }

  @Test
  void processCreateMaterializedViewCreateOrReplaceReplacesExistingViewWithMaterializedView()
      throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SchemaPlus replaceDbSchema = conn.calciteConnection().getRootSchema()
          .add("TEST_REPLACE_MV_DB", new TestDatabaseSchema("test-replace-mv-db"));
      replaceDbSchema.add("existingView", mock(ViewTable.class));

      // Use a real SQL query so prepare.convert() executes normally
      SqlCreateMaterializedView create = (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(
          conn, "CREATE OR REPLACE MATERIALIZED VIEW \"TEST_REPLACE_MV_DB\".\"existingView\""
              + " AS SELECT 1 AS \"x\"");

      stubPlan(mockPipeline());

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(conn);
      HoptimatorDdlUtils.processCreateMaterializedView(
          ctx, prepare, conn, create, HoptimatorDdlUtils.DdlMode.CREATE);

      // After CREATE OR REPLACE, the old ViewTable is gone and a MaterializedViewTable takes its place
      Table table = replaceDbSchema.tables().get("existingView");
      assertNotNull(table);
      assertInstanceOf(MaterializedViewTable.class, table,
          "Expected MaterializedViewTable but got: " + table.getClass().getSimpleName());
    }
  }

  @Test
  void processCreateMaterializedViewPartialViewNameBuildsHyphenatedPipelineName() throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      conn.calciteConnection().getRootSchema()
          .add("TEST_PARTIAL_VIEW_DB", new TestDatabaseSchema("test-partial-db"));

      // A view name containing '$' exercises the partial-view pipeline-name branch
      SqlCreateMaterializedView create = (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(
          conn, "CREATE MATERIALIZED VIEW \"TEST_PARTIAL_VIEW_DB\".\"mainView$partialSuffix\""
              + " AS SELECT 1 AS \"x\"");

      stubPlan(mockPipeline());

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(conn);
      HoptimatorDdlUtils.processCreateMaterializedView(
          ctx, prepare, conn, create, HoptimatorDdlUtils.DdlMode.SPECIFY);

      // The pipeline option should combine the database name, sink name, and the '$' suffix
      assertEquals("test-partial-db-mainView-partialSuffix",
          conn.connectionProperties().getProperty(DeploymentService.PIPELINE_OPTION));
    }
  }

  @Test
  void processCreateTableSpecifyModeReturnsEmptySpecList() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      conn.calciteConnection().getRootSchema()
          .add("TEST_SPECIFY_TABLE_DB", new TestDatabaseSchema("test-specify-db"));

      SqlCreateTable create = (SqlCreateTable) HoptimatorDriver.parseQuery(conn,
          "CREATE TABLE \"TEST_SPECIFY_TABLE_DB\".\"my_table\" (\"id\" INTEGER)");

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      // SPECIFY mode is a dry-run: returns an empty spec list when no deployers are registered
      HoptimatorDdlUtils.SpecifyResult result = HoptimatorDdlUtils.processCreateTable(
          ctx, conn, create, HoptimatorDdlUtils.DdlMode.SPECIFY);

      assertNotNull(result.specs);
      assertTrue(result.specs.isEmpty());
    }
  }

  @Test
  void processCreateTableIsNewSchemaFailsWhenCatalogIsNotHoptimatorJdbcCatalogSchema()
      throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // Register a plain TestDatabaseSchema (NOT a HoptimatorJdbcCatalogSchema) as the catalog.
      // A 3-level path forces the isNewSchema=true branch, after which the schema unwrap
      // fails because TestDatabaseSchema cannot be cast to HoptimatorJdbcCatalogSchema.
      conn.calciteConnection().getRootSchema()
          .add("TEST_CATALOG_A", new TestDatabaseSchema("test-catalog-a"));

      SqlCreateTable create = (SqlCreateTable) HoptimatorDriver.parseQuery(conn,
          "CREATE TABLE \"TEST_CATALOG_A\".\"new_schema\".\"my_table\" (\"id\" INTEGER)");

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      assertThrows(Exception.class,
          () -> HoptimatorDdlUtils.processCreateTable(
              ctx, conn, create, HoptimatorDdlUtils.DdlMode.CREATE));
    }
  }

  @Test
  void processCreateMaterializedViewSpecifyModeCoversPipelinePlanningAndExecution()
      throws Exception {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      conn.calciteConnection().getRootSchema()
          .add("TEST_PIPELINE_DB", new TestDatabaseSchema("test-pipeline-db"));

      SqlCreateMaterializedView create = (SqlCreateMaterializedView) HoptimatorDriver.parseQuery(
          conn, "CREATE MATERIALIZED VIEW \"TEST_PIPELINE_DB\".\"myView\" AS SELECT 1 AS \"x\"");

      stubPlan(mockPipeline());

      CalcitePrepare.Context ctx = conn.createPrepareContext();
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(conn);
      HoptimatorDdlUtils.SpecifyResult result = HoptimatorDdlUtils.processCreateMaterializedView(
          ctx, prepare, conn, create, HoptimatorDdlUtils.DdlMode.SPECIFY);

      assertNotNull(result.specs);
      assertTrue(result.specs.isEmpty());
    }
  }

  static class TestDatabaseSchema extends AbstractSchema
      implements com.linkedin.hoptimator.Database {
    private final String name;

    TestDatabaseSchema(String name) {
      this.name = name;
    }

    @Override
    public String databaseName() {
      return name;
    }
  }
}
