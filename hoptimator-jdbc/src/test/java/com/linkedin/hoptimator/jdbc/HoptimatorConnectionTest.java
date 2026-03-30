package com.linkedin.hoptimator.jdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.RelOptMaterialization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
@ExtendWith(MockitoExtension.class)
class HoptimatorConnectionTest {

  @Mock
  private CalciteConnection mockCalciteConnection;

  @Mock
  private CalcitePrepare.Context mockContext;

  @Mock
  private Statement mockStatement;

  @Mock
  private PreparedStatement mockPreparedStatement;

  @Mock
  private DatabaseMetaData mockDatabaseMetaData;

  private Properties connectionProperties;
  private HoptimatorConnection connection;

  @BeforeEach
  void setUp() {
    connectionProperties = new Properties();
    connectionProperties.setProperty("key1", "value1");
    connection = new HoptimatorConnection(mockCalciteConnection, connectionProperties);
  }

  @Test
  void testConnectionPropertiesReturnsInjectedProperties() {
    Properties result = connection.connectionProperties();

    assertEquals(connectionProperties, result);
    assertEquals("value1", result.getProperty("key1"));
  }

  @Test
  void testCalciteConnectionReturnsOriginal() {
    CalciteConnection result = connection.calciteConnection();

    assertSame(mockCalciteConnection, result);
  }

  @Test
  void testCreateStatementDelegatesToCalciteConnection() throws SQLException {
    when(mockCalciteConnection.createStatement()).thenReturn(mockStatement);

    Statement result = connection.createStatement();

    assertSame(mockStatement, result);
    verify(mockCalciteConnection).createStatement();
  }

  @Test
  void testPrepareStatementDelegatesToCalciteConnection() throws SQLException {
    String sql = "SELECT 1";
    when(mockCalciteConnection.prepareStatement(sql)).thenReturn(mockPreparedStatement);

    PreparedStatement result = connection.prepareStatement(sql);

    assertSame(mockPreparedStatement, result);
    verify(mockCalciteConnection).prepareStatement(sql);
  }

  @Test
  void testGetMetaDataReturnsHoptimatorDatabaseMetaData() throws SQLException {
    when(mockCalciteConnection.getMetaData()).thenReturn(mockDatabaseMetaData);

    DatabaseMetaData result = connection.getMetaData();

    assertNotNull(result);
    assertTrue(result instanceof HoptimatorDatabaseMetaData);
  }

  @Test
  void testCreatePrepareContextDelegatesToCalciteConnection() {
    when(mockCalciteConnection.createPrepareContext()).thenReturn(mockContext);

    CalcitePrepare.Context result = connection.createPrepareContext();

    assertSame(mockContext, result);
    verify(mockCalciteConnection).createPrepareContext();
  }

  @Test
  void testWithPropertiesReturnsNewConnection() {
    Properties newProperties = new Properties();
    newProperties.setProperty("newKey", "newValue");

    HoptimatorConnection newConnection = connection.withProperties(newProperties);

    assertNotNull(newConnection);
    assertEquals("newValue", newConnection.connectionProperties().getProperty("newKey"));
    assertSame(mockCalciteConnection, newConnection.calciteConnection());
  }

  @Test
  void testMaterializationsInitiallyEmpty() {
    List<RelOptMaterialization> materializations = connection.materializations();

    assertNotNull(materializations);
    assertTrue(materializations.isEmpty());
  }

  @Test
  void testAddLogHookIsInvoked() {
    List<String> logged = new ArrayList<>();
    Consumer<String> hook = logged::add;
    connection.addLogHook(hook);

    HoptimatorConnection.HoptimatorConnectionDualLogger logger = connection.getLogger(HoptimatorConnectionTest.class);
    logger.info("test message {}", "arg1");

    assertEquals(1, logged.size());
    assertTrue(logged.get(0).contains("test message arg1"));
    assertTrue(logged.get(0).contains("HoptimatorConnectionTest"));
  }

  @Test
  void testGetLoggerReturnsNonNull() {
    HoptimatorConnection.HoptimatorConnectionDualLogger logger = connection.getLogger(String.class);

    assertNotNull(logger);
  }

  @Test
  @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
      justification = "Connection closed in try-with-resources")
  void testRegisterMaterializationAddsMaterialization() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      conn.registerMaterialization(
          Arrays.asList("UTIL", "PRINT"),
          "SELECT 'hello' AS \"OUTPUT\"");

      List<RelOptMaterialization> materializations = conn.materializations();
      assertEquals(1, materializations.size());
    }
  }

  @Test
  @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
      justification = "Connection closed in try-with-resources")
  void testResolveThrowsForNonDatabaseSchema() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      assertThrows(SQLException.class, () ->
          conn.resolve(Arrays.asList("UTIL", "PRINT"), Collections.emptyMap()));
    }
  }

  @Test
  void testMultipleLogHooksAllInvoked() {
    List<String> hook1Messages = new ArrayList<>();
    List<String> hook2Messages = new ArrayList<>();
    connection.addLogHook(hook1Messages::add);
    connection.addLogHook(hook2Messages::add);

    HoptimatorConnection.HoptimatorConnectionDualLogger logger = connection.getLogger(HoptimatorConnectionTest.class);
    logger.info("hello");

    assertEquals(1, hook1Messages.size());
    assertEquals(1, hook2Messages.size());
  }
}
