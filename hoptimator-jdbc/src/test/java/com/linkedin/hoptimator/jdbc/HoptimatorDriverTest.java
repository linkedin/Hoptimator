package com.linkedin.hoptimator.jdbc;

import com.linkedin.hoptimator.DeploymentContext;
import com.linkedin.hoptimator.Source;
import org.apache.avro.Schema;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class HoptimatorDriverTest {

  private final HoptimatorDriver driver = new HoptimatorDriver();

  @Test
  void testAcceptsUrlWithCorrectPrefix() {
    assertTrue(driver.acceptsURL("jdbc:hoptimator://"));
  }

  @Test
  void testAcceptsUrlWithSuffix() {
    assertTrue(driver.acceptsURL("jdbc:hoptimator://catalogs=util"));
  }

  @Test
  void testRejectsUrlWithWrongPrefix() {
    assertFalse(driver.acceptsURL("jdbc:mysql://localhost"));
  }

  @Test
  void testRejectsEmptyUrl() {
    assertFalse(driver.acceptsURL(""));
  }

  @Test
  void testGetMajorVersion() {
    assertEquals(0, driver.getMajorVersion());
  }

  @Test
  void testGetMinorVersion() {
    assertEquals(1, driver.getMinorVersion());
  }

  @Test
  void testJdbcCompliantReturnsFalse() {
    assertFalse(driver.jdbcCompliant());
  }

  @Test
  void testGetPropertyInfoReturnsEmptyArray() throws SQLException {
    DriverPropertyInfo[] info = driver.getPropertyInfo("jdbc:hoptimator://", new Properties());

    assertNotNull(info);
    assertEquals(0, info.length);
  }

  @Test
  void testGetParentLoggerDoesNotThrow() {
    driver.getParentLogger();
  }

  @Test
  void testConnectReturnsNullForWrongUrl() throws SQLException {
    Connection result = driver.connect("jdbc:mysql://localhost", new Properties());

    assertNull(result);
  }

  @Test
  void testConnectWithValidUrlReturnsConnection() throws SQLException {
    Properties props = new Properties();
    Connection connection = driver.connect("jdbc:hoptimator://", props);

    assertNotNull(connection);
    assertInstanceOf(HoptimatorConnection.class, connection);
    connection.close();
  }

  @Test
  void testConnectWithSpecificCatalogs() throws SQLException {
    Properties props = new Properties();
    Connection connection = driver.connect("jdbc:hoptimator://catalogs=util", props);

    assertNotNull(connection);
    assertInstanceOf(HoptimatorConnection.class, connection);
    connection.close();
  }

  @Test
  void testConnectWithInvalidCatalogThrows() {
    Properties props = new Properties();

    assertThrows(SQLNonTransientException.class, () ->
        driver.connect("jdbc:hoptimator://catalogs=nonexistent", props));
  }

  @Test
  void testRowTypeResolvesTableFromSchema() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      Source source = new Source("UTIL", Arrays.asList("UTIL", "PRINT"), Collections.emptyMap());

      RelDataType rowType = HoptimatorDriver.rowType(source, new CalciteDeploymentContext(connection));

      assertNotNull(rowType);
      assertEquals(1, rowType.getFieldCount());
      assertEquals("OUTPUT", rowType.getFieldNames().get(0));
    }
  }

  @Test
  void testRowTypeThrowsForMissingTable() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      Source source = new Source("UTIL", Arrays.asList("UTIL", "NONEXISTENT"), Collections.emptyMap());

      assertThrows(SQLException.class, () -> HoptimatorDriver.rowType(source, new CalciteDeploymentContext(connection)));
    }
  }

  @Test
  void testRowTypeDerivedFromAvroSchemaOnDirectContext() throws SQLException {
    Schema avro = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\","
        + "\"namespace\":\"com.example\",\"fields\":[{\"name\":\"ID\",\"type\":\"int\"}]}");
    DirectDeploymentContext context = new DirectDeploymentContext(new Properties(), null, avro);
    Source source = new Source("KAFKA", Arrays.asList("KAFKA", "my_topic"), Collections.emptyMap());

    RelDataType rowType = HoptimatorDriver.rowType(source, context);
    assertTrue(rowType.isStruct());
    assertTrue(rowType.getFieldNames().contains("ID"));
  }

  @Test
  void testRowTypeThrowsForUnknownContextType() {
    Source source = new Source("KAFKA", Arrays.asList("KAFKA", "my_topic"), Collections.emptyMap());
    DeploymentContext unknown = new DeploymentContext() {
      @Override
      public Properties properties() {
        return new Properties();
      }

      @Override
      public Properties databaseProperties(String catalog, String schema, String connectionPrefix) {
        return null;
      }
    };

    assertThrows(SQLException.class, () -> HoptimatorDriver.rowType(source, unknown));
  }

  @Test
  void testValueSchemaReturnsNullForSchemaFreeDirectContext() {
    DirectDeploymentContext context = new DirectDeploymentContext(new Properties(), null, null);
    Source source = new Source("KAFKA", Arrays.asList("KAFKA", "my_topic"), Collections.emptyMap());

    assertNull(HoptimatorDriver.valueSchema(source, context));
  }

  @Test
  void testValueSchemaSplitsValueFromCarriedMergedAvroOnDirectContext() {
    // The caller's carried Avro is a merged key+value record (KEY_-prefixed key fields). valueSchema
    // must return only the value portion (keys handled separately via key.fields) and preserve a
    // nested record's namespace losslessly rather than re-synthesizing from the flat row type.
    Schema merged = new Schema.Parser().parse("{"
        + "\"type\":\"record\",\"name\":\"StoreValue\",\"namespace\":\"com.example.kafka\",\"fields\":["
        + "{\"name\":\"KEY_id\",\"type\":\"int\"},"
        + "{\"name\":\"widget\",\"type\":{\"type\":\"record\",\"name\":\"Widget\","
        + "\"namespace\":\"com.example.custom\",\"fields\":[{\"name\":\"w\",\"type\":\"string\"}]}}"
        + "]}");
    DirectDeploymentContext context = new DirectDeploymentContext(new Properties(), null, merged);
    Source source = new Source("KAFKA", Arrays.asList("KAFKA", "my_topic"), Collections.emptyMap());

    Schema value = HoptimatorDriver.valueSchema(source, context);

    assertNull(value.getField("KEY_id"), "key field excluded from value schema");
    assertNotNull(value.getField("widget"), "value field retained");
    assertEquals("com.example.custom", value.getField("widget").schema().getNamespace(),
        "nested namespace preserved losslessly");
  }

  @Test
  void testProvidedAvroSchemaReturnsCarriedSchemaFromDirectContext() {
    Schema avro = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"R\","
        + "\"namespace\":\"com.example\",\"fields\":[{\"name\":\"ID\",\"type\":\"int\"}]}");
    DirectDeploymentContext context = new DirectDeploymentContext(new Properties(), null, avro);
    Source source = new Source("KAFKA", Arrays.asList("KAFKA", "my_topic"), Collections.emptyMap());

    assertSame(avro, HoptimatorDriver.providedAvroSchema(source, context));
  }

  @Test
  void testProvidedAvroSchemaReturnsNullWhenNoneCarried() {
    DirectDeploymentContext context = new DirectDeploymentContext(new Properties(), null, null);
    Source source = new Source("KAFKA", Arrays.asList("KAFKA", "my_topic"), Collections.emptyMap());

    assertNull(HoptimatorDriver.providedAvroSchema(source, context));
  }

  @Test
  void testValueSchemaReturnsNullForTableWithoutNativeAvro() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // UTIL.PRINT is a real catalog table but not an AvroSchemaSource, so no native value schema.
      Source source = new Source("UTIL", Arrays.asList("UTIL", "PRINT"), Collections.emptyMap());

      Schema schema = HoptimatorDriver.valueSchema(source, new CalciteDeploymentContext(connection));

      assertNull(schema);
    }
  }

  @Test
  void testParseQueryReturnsNode() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      SqlNode node = HoptimatorDriver.parseQuery(connection, "SELECT 1");

      assertNotNull(node);
    }
  }

  @Test
  void testParseQueryThrowsForInvalidSql() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      assertThrows(RuntimeException.class, () ->
          HoptimatorDriver.parseQuery(connection, "NOT VALID SQL %%%"));
    }
  }

  @Test
  void testConvertReturnsPlan() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.ConvertResult result =
          HoptimatorDriver.convert(connection, "SELECT * FROM \"UTIL\".\"PRINT\"");

      assertNotNull(result);
      assertNotNull(result.root);
    }
  }

  @Test
  void testAnalyzeViewReturnsResult() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.AnalyzeViewResult result =
          HoptimatorDriver.analyzeView(connection, "SELECT 1");

      assertNotNull(result);
    }
  }

  @Test
  void testConnectionNotClosed() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      assertFalse(connection.isClosed());
    }
  }

  @Test
  void testGetMetaDataReturnsDatabaseMetaData() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      DatabaseMetaData metaData = connection.getMetaData();

      assertNotNull(metaData);
      assertInstanceOf(HoptimatorDatabaseMetaData.class, metaData);
    }
  }

  @Test
  void testCreateStatementWorks() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      Statement stmt = connection.createStatement();

      assertNotNull(stmt);
      stmt.close();
    }
  }

  @Test
  void testPrepareStatementWorks() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      PreparedStatement pstmt = connection.prepareStatement("SELECT 1");

      assertNotNull(pstmt);
      pstmt.close();
    }
  }

  @Test
  void testCreatePrepareContextReturnsContext() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      CalcitePrepare.Context ctx = connection.createPrepareContext();

      assertNotNull(ctx);
      assertNotNull(ctx.getRootSchema());
    }
  }

  @Test
  void testPrepareClassConstructorWithConnection() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      HoptimatorDriver.Prepare prepare = new HoptimatorDriver.Prepare(connection);

      assertNotNull(prepare);
    }
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
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      String schema = connection.getSchema();

      assertNotNull(schema);
    }
  }

  @Test
  void connectSetsAutoCommitTrue() throws SQLException {
    try (Connection connection = driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      assertTrue(connection.getAutoCommit(), "AutoCommit must be set to true by connect()");
    }
  }

  // If the catalog is never added, querying a table in it will throw
  @Test
  void connectWithCatalogMakesTablesQueryable() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM \"UTIL\".\"PRINT\"")) {
        assertNotNull(ps, "Prepared statement must succeed — catalog must be registered in root schema");
      }
    }
  }

  // Connecting twice to same catalog must succeed
  @Test
  void connectTwiceWithSameCatalogSucceeds() throws SQLException {
    try (Connection c1 = driver.connect("jdbc:hoptimator://catalogs=util", new Properties());
         Connection c2 = driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      assertNotNull(c1);
      assertNotNull(c2);
    }
  }

  // Without the schema-add guard, catalogs param is ignored
  @Test
  void connectWithCatalogParamOnlyLoadsSpecifiedCatalog() throws SQLException {
    try (HoptimatorConnection connection =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // UTIL catalog must be accessible
      try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM \"UTIL\".\"PRINT\"")) {
        assertNotNull(ps);
      }
    }
  }

  // onConnectionInit sets up the connection state; without it the connection behaves unexpectedly
  @Test
  void calciteDriverConnectReturnsUsableConnection() throws SQLException {
    CalciteDriver calciteDriver = new CalciteDriver();
    try (Connection connection = calciteDriver.connect("jdbc:calcite:", new Properties())) {
      // onConnectionInit must be called for the connection to be non-null and functional
      assertNotNull(connection, "CalciteDriver.connect() must return a non-null connection after onConnectionInit");
      assertFalse(connection.isClosed());
    }
  }
}
