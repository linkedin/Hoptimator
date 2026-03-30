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
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
