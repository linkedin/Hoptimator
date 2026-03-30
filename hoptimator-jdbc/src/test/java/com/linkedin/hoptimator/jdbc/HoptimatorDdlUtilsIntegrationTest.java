package com.linkedin.hoptimator.jdbc;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


class HoptimatorDdlUtilsIntegrationTest {

  private HoptimatorConnection connection;

  @BeforeEach
  void setUp() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    Properties props = new Properties();
    connection = (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", props);
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void testSchemaWithSimpleIdentifier() {
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlIdentifier id = new SqlIdentifier("myTable", SqlParserPos.ZERO);

    Pair<CalciteSchema, String> result = HoptimatorDdlUtils.schema(context, false, id);

    assertNotNull(result);
    assertEquals("myTable", result.right);
  }

  @Test
  void testSchemaWithCompoundIdentifier() {
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlIdentifier id = new SqlIdentifier(Arrays.asList("util", "PRINT"), SqlParserPos.ZERO);

    Pair<CalciteSchema, String> result = HoptimatorDdlUtils.schema(context, false, id);

    assertNotNull(result);
    assertEquals("PRINT", result.right);
  }

  @Test
  void testSchemaWithMutableFlag() {
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlIdentifier id = new SqlIdentifier("myTable", SqlParserPos.ZERO);

    Pair<CalciteSchema, String> result = HoptimatorDdlUtils.schema(context, true, id);

    assertNotNull(result);
    assertEquals("myTable", result.right);
  }

  @Test
  void testCatalogWithThreePartIdentifier() {
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlIdentifier id = new SqlIdentifier(Arrays.asList("catalog", "schema", "table"), SqlParserPos.ZERO);

    Pair<CalciteSchema, String> result = HoptimatorDdlUtils.catalog(context, false, id);

    assertNotNull(result);
    assertEquals("schema.table", result.right);
  }

  @Test
  void testCatalogThrowsForTwoPartIdentifier() {
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlIdentifier id = new SqlIdentifier(Arrays.asList("schema", "table"), SqlParserPos.ZERO);

    assertThrows(IllegalArgumentException.class,
        () -> HoptimatorDdlUtils.catalog(context, false, id));
  }

  @Test
  void testCatalogThrowsForSimpleIdentifier() {
    CalcitePrepare.Context context = connection.createPrepareContext();
    SqlIdentifier id = new SqlIdentifier("table", SqlParserPos.ZERO);

    assertThrows(IllegalArgumentException.class,
        () -> HoptimatorDdlUtils.catalog(context, false, id));
  }

  @Test
  void testViewTableCreation() {
    CalcitePrepare.Context context = connection.createPrepareContext();
    HoptimatorDriver.Prepare impl = new HoptimatorDriver.Prepare(connection);

    ViewTable viewTable = HoptimatorDdlUtils.viewTable(
        context, "SELECT 1", impl,
            List.of("DEFAULT"), Arrays.asList("DEFAULT", "myView"));

    assertNotNull(viewTable);
  }
}
