package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.jdbc.ddl.SqlCreateMaterializedView;
import com.linkedin.hoptimator.util.planner.PipelineRel;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class HoptimatorDdlUtilsTest {

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
