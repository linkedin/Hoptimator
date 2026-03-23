package com.linkedin.hoptimator.jdbc;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.Source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Connections in tests are closed in try-with-resources or after assertions")
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
    assertTrue(connection instanceof HoptimatorConnection);
    connection.close();
  }

  @Test
  void testConnectWithSpecificCatalogs() throws SQLException {
    Properties props = new Properties();
    Connection connection = driver.connect("jdbc:hoptimator://catalogs=util", props);

    assertNotNull(connection);
    assertTrue(connection instanceof HoptimatorConnection);
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

      RelDataType rowType = HoptimatorDriver.rowType(source, connection);

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

      assertThrows(SQLException.class, () -> HoptimatorDriver.rowType(source, connection));
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
}
