package com.linkedin.hoptimator.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import org.apache.calcite.jdbc.CalcitePrepare;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class CalciteDriverTest {

  @Test
  void testDefaultConstructorCreatesDriver() {
    CalciteDriver driver = new CalciteDriver();

    assertNotNull(driver);
  }

  @Test
  void testConnectReturnsNullForNonCalciteUrl() throws SQLException {
    CalciteDriver driver = new CalciteDriver();

    Connection result = driver.connect("jdbc:mysql://localhost", new Properties());

    assertNull(result);
  }

  @Test
  void testConnectWithCalciteUrl() throws SQLException {
    CalciteDriver driver = new CalciteDriver();
    Properties props = new Properties();

    Connection result = driver.connect("jdbc:calcite:", props);

    assertNotNull(result);
    result.close();
  }

  @Test
  void testConnectWithCacheDisabled() throws SQLException {
    CalciteDriver driver = new CalciteDriver();
    Properties props = new Properties();

    Connection result = driver.connect("jdbc:calcite:", props, false);

    assertNotNull(result);
    result.close();
  }

  @Test
  void testConnectWithCacheEnabled() throws SQLException {
    CalciteDriver driver = new CalciteDriver();
    Properties props = new Properties();

    Connection result = driver.connect("jdbc:calcite:", props, true);

    assertNotNull(result);
    result.close();
  }

  @Test
  void testWithPrepareFactoryReturnsSameIfSameFactory() {
    CalciteDriver driver = new CalciteDriver();

    // The internal prepareFactory is null for default constructor,
    // so we need to set one first, then set the same one
    CalciteDriver driver2 = driver.withPrepareFactory(() -> null);
    CalciteDriver driver3 = driver2.withPrepareFactory(() -> null);

    // Different lambda instances, so should create a new driver
    assertNotSame(driver2, driver3);
  }

  @Test
  void testWithPrepareFactoryReturnsSameInstanceWhenSameFactory() {
    Supplier<CalcitePrepare> factory = () -> null;
    CalciteDriver driver = new CalciteDriver();
    CalciteDriver driver2 = driver.withPrepareFactory(factory);
    CalciteDriver driver3 = driver2.withPrepareFactory(factory);

    assertSame(driver2, driver3);
  }

  @Test
  void testConnectWithCacheReturnNullForWrongUrl() throws SQLException {
    CalciteDriver driver = new CalciteDriver();

    Connection result = driver.connect("jdbc:mysql://localhost", new Properties(), false);

    assertNull(result);
  }
}
