package com.linkedin.hoptimator.jdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.avro.AvroSchemaSource;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(
    value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Tests use when(mock.createStatement()/prepareStatement()).thenReturn(...) "
        + "to stub and verify(mock).prepareStatement(...) to verify. Both are Mockito DSL "
        + "invocations on methods whose AutoCloseable return types SpotBugs flags. There is no "
        + "way to express these in the Mockito API without the method invocation on the mock.")
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
    assertInstanceOf(HoptimatorDatabaseMetaData.class, result);
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
  void testResolveThrowsForNonDatabaseSchema() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      assertThrows(SQLException.class, () ->
          conn.resolve(Arrays.asList("UTIL", "PRINT"), Collections.emptyMap()));
    }
  }

  @Test
  void resolveReturnsNonNullTypeForExistingTwoPartPath() throws SQLException {
    HoptimatorDriver driver = new HoptimatorDriver();
    try (HoptimatorConnection conn =
        (HoptimatorConnection) driver.connect("jdbc:hoptimator://catalogs=util", new Properties())) {
      // "UTIL.PRINT" is the known 2-part path in the util catalog
      RelDataType result =
          HoptimatorDriver.rowType(
              new Source("UTIL", Arrays.asList("UTIL", "PRINT"), Collections.emptyMap()),
              conn);
      assertNotNull(result, "resolve() must find the PRINT table");
      assertTrue(result.getFieldCount() > 0, "resolve() must return a non-empty row type");
      // "OUTPUT" is a known field in UTIL.PRINT — ensures correct path math, not empty Optional
      boolean hasOutput = result.getFieldNames().stream().anyMatch(n -> n.contains("OUTPUT"));
      assertTrue(hasOutput, "resolve() must resolve to UTIL.PRINT with its OUTPUT field");
    }
  }

  @Test
  void avroSchemaAtReturnsNullForUnknownPath() {
    SchemaPlus root = CalciteSchema.createRootSchema(false).plus();
    assertNull(HoptimatorConnection.avroSchemaAt(root,
        List.of("missingDb", "missingTable")));
  }

  @Test
  void avroSchemaAtReturnsNullWhenTableIsNotSource() {
    SchemaPlus root = CalciteSchema.createRootSchema(false).plus();
    SchemaPlus db = root.add("db", new AbstractSchema());
    db.add("plain", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory factory) {
        throw new UnsupportedOperationException();
      }
    });

    assertNull(HoptimatorConnection.avroSchemaAt(root, List.of("db", "plain")));
  }

  @Test
  void avroSchemaAtReturnsValueSchemaWhenSourceHasNoKey() {
    // No key → the merge helper returns the value schema unchanged.
    Schema value = SchemaBuilder.record("User").namespace("com.linkedin.foo").fields()
        .requiredString("name").endRecord();
    SchemaPlus root = CalciteSchema.createRootSchema(false).plus();
    SchemaPlus db = root.add("db", new AbstractSchema());
    db.add("user", new SourceTable(value, null));

    Schema result = HoptimatorConnection.avroSchemaAt(root, List.of("db", "user"));

    assertSame(value, result, "no key → merged is just the value schema");
  }

  @Test
  void avroSchemaAtMergesKeyAndValueWhenSourceExposesBoth() {
    // Both key and value → merged view with KEY_-prefixed key fields prepended before value.
    // resolve() uses this so SQL queries can reference key columns by name.
    Schema value = SchemaBuilder.record("User").namespace("com.linkedin.foo").fields()
        .requiredString("name").endRecord();
    Schema key = SchemaBuilder.record("UserKey").namespace("com.linkedin.keyns").fields()
        .requiredString("id").endRecord();
    SchemaPlus root = CalciteSchema.createRootSchema(false).plus();
    SchemaPlus db = root.add("db", new AbstractSchema());
    db.add("user", new SourceTable(value, key));

    Schema result = HoptimatorConnection.avroSchemaAt(root, List.of("db", "user"));

    assertNotNull(result);
    assertEquals(2, result.getFields().size());
    assertEquals("KEY_id", result.getFields().get(0).name());
    assertEquals("name", result.getFields().get(1).name());
    // Merged record inherits value's namespace/name.
    assertEquals("com.linkedin.foo", result.getNamespace());
    assertEquals("User", result.getName());
  }

  @Test
  void avroSchemaAtMergesPrimitiveKeyAsSingleKeyField() {
    Schema value = SchemaBuilder.record("User").namespace("com.linkedin.foo").fields()
        .requiredString("name").endRecord();
    Schema key = Schema.create(Schema.Type.STRING);
    SchemaPlus root = CalciteSchema.createRootSchema(false).plus();
    SchemaPlus db = root.add("db", new AbstractSchema());
    db.add("user", new SourceTable(value, key));

    Schema result = HoptimatorConnection.avroSchemaAt(root, List.of("db", "user"));

    assertNotNull(result);
    assertEquals(2, result.getFields().size());
    assertEquals("KEY", result.getFields().get(0).name());
    assertEquals(Schema.Type.STRING, result.getFields().get(0).schema().getType());
    assertEquals("name", result.getFields().get(1).name());
  }

  private static final class SourceTable extends AbstractTable implements AvroSchemaSource {
    private final Schema value;
    private final Schema key;

    SourceTable(Schema value, Schema key) {
      this.value = value;
      this.key = key;
    }

    @Override
    public Schema valueSchema() {
      return value;
    }

    @Override
    public Schema keySchema() {
      return key;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      throw new UnsupportedOperationException();
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
