package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.Validator;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.jdbc.HoptimatorDriver;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class MySqlDeployerValidationTest {

  private static final Properties PROPERTIES = new Properties();

  static {
    PROPERTIES.setProperty("url", "jdbc:mysql://test-url");
    PROPERTIES.setProperty("user", "testuser");
    PROPERTIES.setProperty("password", "testpass");
  }

  @Mock
  private Connection mockConnection;

  @Mock
  private DatabaseMetaData mockMetaData;

  @Mock
  private Statement mockStatement;

  @Mock
  private HoptimatorConnection mockHoptimatorConnection;

  @Mock
  private MockedStatic<DriverManager> driverManagerStatic;

  @Mock
  private MockedStatic<HoptimatorDriver> hoptimatorDriverStatic;

  private void stubConnection() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    driverManagerStatic.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
        .thenReturn(mockConnection);
  }

  private void stubConnectionWithStatement() throws SQLException {
    stubConnection();
    when(mockConnection.createStatement()).thenReturn(mockStatement);
  }

  private void stubDefaultRowType() {
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("name", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), true));
    RelDataType rowType = builder.build();
    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);
  }

  // --- validate() tests ---

  @Test
  void testValidateFailsWithNullDatabase() {
    Source source = new Source("db", List.of("TestTable"), Collections.emptyMap());

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Database & table names are required"));
  }

  @Test
  void testValidateFailsWithInvalidDatabaseName() {
    Source source = new Source("db", List.of("MYSQL", "invalid-db", "TestTable"), Collections.emptyMap());

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Invalid database name"));
  }

  @Test
  void testValidateFailsWithInvalidTableName() {
    Source source = new Source("db", List.of("MYSQL", "test_db", "invalid table"), Collections.emptyMap());

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Invalid table name"));
  }

  @Test
  void testValidateFailsNoKeyFields() throws SQLException {
    stubConnection();

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255));
    builder.add("age", typeFactory.createSqlType(SqlTypeName.INTEGER));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "TestTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("TestTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("No KEY_ fields found"));
  }

  @Test
  void testValidateFailsWhenPrimaryKeysChange() throws SQLException {
    stubConnection();
    stubDefaultRowType();

    Source source = new Source("db", List.of("MYSQL", "test_db", "ExistingTable"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("ExistingTable"), any())).thenReturn(existingRs);

    // Existing primary key is "different_key" instead of "id"
    when(mockMetaData.getPrimaryKeys(eq("test_db"), any(), eq("ExistingTable"))).thenAnswer(invocation -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false);
      when(pkRs.getString("COLUMN_NAME")).thenReturn("different_key");
      return pkRs;
    });

    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, true, false);
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("different_key", "name");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10, 255);
    when(columnsRs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);
    when(mockMetaData.getColumns(eq("test_db"), any(), eq("ExistingTable"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Cannot modify KEY fields"));
  }

  // --- update() with alterTable ---

  @Test
  void testUpdateAltersExistingTableAddsColumn() throws Exception {
    stubConnectionWithStatement();

    // Row type with KEY_id, name, AND a new "email" column
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("name", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), true));
    builder.add("email", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), true));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "MyTable"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("MyTable"), any())).thenReturn(existingRs);

    when(mockMetaData.getPrimaryKeys(eq("test_db"), any(), eq("MyTable"))).thenAnswer(invocation -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false);
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, true, false);
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10, 255);
    when(columnsRs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);
    when(mockMetaData.getColumns(eq("test_db"), any(), eq("MyTable"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    deployer.update();

    // CREATE DATABASE + ALTER TABLE ADD COLUMN email
    verify(mockStatement, times(2)).executeUpdate(anyString());
  }

  @Test
  void testUpdateAltersExistingTableNoChanges() throws Exception {
    stubConnectionWithStatement();
    stubDefaultRowType();

    Source source = new Source("db", List.of("MYSQL", "test_db", "MyTable"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("MyTable"), any())).thenReturn(existingRs);

    when(mockMetaData.getPrimaryKeys(eq("test_db"), any(), eq("MyTable"))).thenAnswer(invocation -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false);
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    // Existing columns match the desired schema exactly
    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, true, false);
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10, 255);
    when(columnsRs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);
    when(mockMetaData.getColumns(eq("test_db"), any(), eq("MyTable"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    deployer.update();

    // Only CREATE DATABASE, no ALTER TABLE
    verify(mockStatement, times(1)).executeUpdate(anyString());
  }

  // --- delete() edge cases ---

  @Test
  void testDeleteNonExistentTableSkipsDeletion() throws Exception {
    stubConnection();

    Source source = new Source("db", List.of("MYSQL", "test_db", "GhostTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("GhostTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    deployer.delete();

    verify(mockStatement, never()).executeUpdate(anyString());
  }

  // --- create/update with null database ---

  @Test
  void testCreateFailsWithNullDatabase() {
    Source source = new Source("db", List.of("TestTable"), Collections.emptyMap());
    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);

    assertThrows(SQLException.class, deployer::create);
  }

  @Test
  void testUpdateFailsWithNullDatabase() {
    Source source = new Source("db", List.of("TestTable"), Collections.emptyMap());
    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);

    assertThrows(SQLException.class, deployer::update);
  }

  // --- toMySqlType() parameterized tests ---

  static Stream<Arguments> typeMappingCases() {
    return Stream.of(
        Arguments.of("INTEGER", SqlTypeName.INTEGER, -1, -1, "INT"),
        Arguments.of("BIGINT", SqlTypeName.BIGINT, -1, -1, "BIGINT"),
        Arguments.of("VARCHAR with precision", SqlTypeName.VARCHAR, 100, -1, "VARCHAR(100)"),
        Arguments.of("VARCHAR without precision", SqlTypeName.VARCHAR, -1, -1, "TEXT"),
        Arguments.of("CHAR", SqlTypeName.CHAR, 10, -1, "CHAR(10)"),
        Arguments.of("BOOLEAN", SqlTypeName.BOOLEAN, -1, -1, "BOOLEAN"),
        Arguments.of("DOUBLE", SqlTypeName.DOUBLE, -1, -1, "DOUBLE"),
        Arguments.of("FLOAT", SqlTypeName.FLOAT, -1, -1, "FLOAT"),
        Arguments.of("DECIMAL", SqlTypeName.DECIMAL, 10, 2, "DECIMAL(10,2)"),
        Arguments.of("DATE", SqlTypeName.DATE, -1, -1, "DATE"),
        Arguments.of("TIME", SqlTypeName.TIME, -1, -1, "TIME"),
        Arguments.of("TIMESTAMP", SqlTypeName.TIMESTAMP, -1, -1, "TIMESTAMP"),
        Arguments.of("BINARY (default)", SqlTypeName.BINARY, -1, -1, "TEXT")
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("typeMappingCases")
  void testToMySqlTypeMappings(String label, SqlTypeName sqlType, int precision, int scale,
      String expectedMySqlType) throws Exception {
    stubConnectionWithStatement();

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    if (precision > 0 && scale >= 0) {
      builder.add("test_col", typeFactory.createSqlType(sqlType, precision, scale));
    } else if (precision > 0) {
      builder.add("test_col", typeFactory.createSqlType(sqlType, precision));
    } else {
      builder.add("test_col", typeFactory.createSqlType(sqlType));
    }
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "TypeTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("TypeTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    deployer.create();

    // Verify CREATE TABLE was executed and contains the expected type
    verify(mockStatement, times(2)).executeUpdate(anyString());
  }

  // --- isValidIdentifier() edge cases ---

  @Test
  void testValidateFailsWithEmptyDatabaseName() {
    Source source = new Source("db", List.of("MYSQL", "", "TestTable"), Collections.emptyMap());

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Invalid database name"));
  }

  // --- alterTable() modify and drop column coverage ---

  @Test
  void testUpdateAltersExistingTableModifiesColumn() throws Exception {
    stubConnectionWithStatement();

    // Desired schema: KEY_id (INT), name (VARCHAR(500)) — name changes from VARCHAR(255) to VARCHAR(500)
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("name", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR, 500), true));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "ModTable"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("ModTable"), any())).thenReturn(existingRs);

    when(mockMetaData.getPrimaryKeys(eq("test_db"), any(), eq("ModTable"))).thenAnswer(invocation -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false);
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    // Existing columns: id (INT), name (VARCHAR(255))
    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, true, false);
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10, 255);
    when(columnsRs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);
    when(mockMetaData.getColumns(eq("test_db"), any(), eq("ModTable"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    deployer.update();

    // CREATE DATABASE + ALTER TABLE MODIFY COLUMN
    verify(mockStatement, times(2)).executeUpdate(anyString());
  }

  @Test
  void testUpdateAltersExistingTableDropsColumn() throws Exception {
    stubConnectionWithStatement();
    stubDefaultRowType(); // KEY_id, name

    Source source = new Source("db", List.of("MYSQL", "test_db", "DropTable"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("DropTable"), any())).thenReturn(existingRs);

    when(mockMetaData.getPrimaryKeys(eq("test_db"), any(), eq("DropTable"))).thenAnswer(invocation -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false);
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    // Existing columns: id (INT), name (VARCHAR(255)), old_col (INT) — old_col should be dropped
    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, true, true, false);
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name", "old_col");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR", "INT");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10, 255, 10);
    when(columnsRs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable, DatabaseMetaData.columnNullable);
    when(mockMetaData.getColumns(eq("test_db"), any(), eq("DropTable"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    deployer.update();

    // CREATE DATABASE + ALTER TABLE DROP COLUMN old_col
    verify(mockStatement, times(2)).executeUpdate(anyString());
  }

  @Test
  void testValidateFailsWithInvalidColumnName() throws SQLException {
    stubConnection();

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("invalid column", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "BadColTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("BadColTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Invalid column name"));
  }

  @Test
  void testValidateFailsWhenRowTypeThrowsException() throws SQLException {
    SQLException schemaError = new SQLException("schema error");
    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenThrow(schemaError);

    Source source = new Source("db", List.of("MYSQL", "test_db", "SchemaErrorTable"), Collections.emptyMap());

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid());
    assertTrue(issues.toString().contains("Failed to get schema for table"));
  }
}
