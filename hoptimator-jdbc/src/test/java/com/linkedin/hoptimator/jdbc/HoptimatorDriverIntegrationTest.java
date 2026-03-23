package com.linkedin.hoptimator.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.sql.PreparedStatement;

import com.linkedin.hoptimator.Validator;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class HoptimatorDriverIntegrationTest {

  private HoptimatorDriver driver;
  private HoptimatorConnection connection;

  @BeforeEach
  void setUp() throws SQLException {
    driver = new HoptimatorDriver();
    Properties props = new Properties();
    connection = (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", props);
  }

  @AfterEach
  void tearDown() throws SQLException {
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }
  }

  @Test
  void testParseQueryReturnsValidNode() {
    SqlNode node = HoptimatorDriver.parseQuery(connection, "SELECT 1");

    assertNotNull(node);
  }

  @Test
  void testParseQueryThrowsForInvalidSql() {
    assertThrows(RuntimeException.class,
        () -> HoptimatorDriver.parseQuery(connection, "NOT VALID SQL !!!"));
  }

  @Test
  void testConvertReturnsResult() {
    CalcitePrepare.ConvertResult result =
        HoptimatorDriver.convert(connection, "SELECT 1");

    assertNotNull(result);
    assertNotNull(result.root);
  }

  @Test
  void testAnalyzeViewReturnsResult() {
    CalcitePrepare.AnalyzeViewResult result =
        HoptimatorDriver.analyzeView(connection, "SELECT 1");

    assertNotNull(result);
  }

  @Test
  void testConnectWithPropertiesInUrl() throws SQLException {
    Properties props = new Properties();
    Connection conn = driver.connect("jdbc:hoptimator://catalogs=util", props);

    assertNotNull(conn);
    assertTrue(conn instanceof HoptimatorConnection);
    conn.close();
  }

  @Test
  void testConnectWithAllCatalogs() throws SQLException {
    Properties props = new Properties();
    Connection conn = driver.connect("jdbc:hoptimator://", props);

    assertNotNull(conn);
    assertTrue(conn instanceof HoptimatorConnection);
    conn.close();
  }

  @Test
  void testConnectionNotClosed() throws SQLException {
    assertFalse(connection.isClosed());
  }

  @Test
  void testGetMetaDataReturnsDatabaseMetaData() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();

    assertNotNull(metaData);
    assertTrue(metaData instanceof HoptimatorDatabaseMetaData);
  }

  @Test
  void testCreateStatementWorks() throws SQLException {
    Statement stmt = connection.createStatement();

    assertNotNull(stmt);
    stmt.close();
  }

  @Test
  void testPrepareStatementWorks() throws SQLException {
    PreparedStatement pstmt = connection.prepareStatement("SELECT 1");

    assertNotNull(pstmt);
    pstmt.close();
  }

  @Test
  void testValidationServiceWithRealConnection() {
    CalciteConnection calciteConn = connection.calciteConnection();
    Validator.Issues issues = ValidationService.validate(calciteConn);

    assertNotNull(issues);
  }

  @Test
  void testCreatePrepareContextReturnsContext() {
    CalcitePrepare.Context ctx = connection.createPrepareContext();

    assertNotNull(ctx);
    assertNotNull(ctx.getRootSchema());
  }

  @Test
  void testPrepareClassConstructorWithConnection() {
    HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);

    assertNotNull(prepare);
  }

  @Test
  void testPrepareParserConfig() {
    HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);

    assertNotNull(prepare);
    // The prepare has the Hoptimator DDL parser factory
  }

  @Test
  void testConnectWithPropertiesObject() throws SQLException {
    Properties props = new Properties();
    props.setProperty("catalogs", "util");
    Connection conn = driver.connect("jdbc:hoptimator://", props);

    assertNotNull(conn);
    conn.close();
  }

  @Test
  void testConnectionSchemaIsDefault() throws SQLException {
    String schema = connection.getSchema();

    assertNotNull(schema);
  }
}
