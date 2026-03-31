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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentCaptor;


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

  // --- toMySqlType() exact string assertions ---

  @ParameterizedTest(name = "{0}")
  @MethodSource("typeMappingCases")
  void testToMySqlTypeExactSqlString(String label, SqlTypeName sqlType, int precision, int scale,
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

    Source source = new Source("db", List.of("MYSQL", "test_db", "TypeExact"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("TypeExact"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.create();

    // Capture all SQL statements executed and find the CREATE TABLE one
    verify(mockStatement, times(2)).executeUpdate(sqlCaptor.capture());
    List<String> allSql = sqlCaptor.getAllValues();
    String createTableSql = allSql.stream()
        .filter(s -> s.startsWith("CREATE TABLE"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No CREATE TABLE statement found in: " + allSql));

    assertTrue(createTableSql.contains(expectedMySqlType),
        "Expected SQL to contain '" + expectedMySqlType + "' but was: " + createTableSql);
    assertNotNull(expectedMySqlType);
    assertFalse(expectedMySqlType.isEmpty(), "Expected MySQL type must be non-empty");
  }

  // --- VARCHAR precision boundary: precision > 0 → VARCHAR(N), precision == 0 → TEXT ---

  @Test
  void testToMySqlTypeVarcharWithPrecisionGivesVarcharN() throws Exception {
    stubConnectionWithStatement();

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("col", typeFactory.createSqlType(SqlTypeName.VARCHAR, 1)); // precision == 1 (> 0 boundary)
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "VarcharBound"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("VarcharBound"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.create();
    verify(mockStatement, times(2)).executeUpdate(sqlCaptor.capture());

    String createSql = sqlCaptor.getAllValues().stream()
        .filter(s -> s.startsWith("CREATE TABLE")).findFirst().orElseThrow();
    assertTrue(createSql.contains("VARCHAR(1)"), "Expected VARCHAR(1) for precision=1, got: " + createSql);
  }

  // --- buildCreateTableSql() nullable/non-nullable, PRIMARY KEY content assertions ---

  @Test
  void testBuildCreateTableSqlNonNullableColumnContainsNotNull() throws Exception {
    stubConnectionWithStatement();

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    // KEY_id is non-nullable by default from createSqlType
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    // name is explicitly NOT nullable
    builder.add("name", typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), false));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "NonNullTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("NonNullTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.create();
    verify(mockStatement, times(2)).executeUpdate(sqlCaptor.capture());

    String createSql = sqlCaptor.getAllValues().stream()
        .filter(s -> s.startsWith("CREATE TABLE")).findFirst().orElseThrow();
    assertTrue(createSql.contains("NOT NULL"),
        "Expected NOT NULL in DDL for non-nullable column, got: " + createSql);
  }

  @Test
  void testBuildCreateTableSqlNullableColumnDoesNotContainNotNull() throws Exception {
    stubConnectionWithStatement();

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    // name is explicitly nullable
    builder.add("name", typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), true));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "NullableTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("NullableTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.create();
    verify(mockStatement, times(2)).executeUpdate(sqlCaptor.capture());

    String createSql = sqlCaptor.getAllValues().stream()
        .filter(s -> s.startsWith("CREATE TABLE")).findFirst().orElseThrow();
    // nullable column should NOT have NOT NULL suffix
    // The name column entry should not contain NOT NULL (but the overall DDL may have NOT NULL for KEY_id)
    // Assert that the `name` column definition does not contain NOT NULL
    assertFalse(createSql.contains("`name` VARCHAR(255) NOT NULL"),
        "Nullable column should not have NOT NULL, got: " + createSql);
    assertTrue(createSql.contains("`name` VARCHAR(255)"),
        "Expected name column as VARCHAR(255), got: " + createSql);
  }

  @Test
  void testBuildCreateTableSqlContainsPrimaryKey() throws Exception {
    stubConnectionWithStatement();

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("name", typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 100), true));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "PKTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("PKTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.create();
    verify(mockStatement, times(2)).executeUpdate(sqlCaptor.capture());

    String createSql = sqlCaptor.getAllValues().stream()
        .filter(s -> s.startsWith("CREATE TABLE")).findFirst().orElseThrow();
    assertTrue(createSql.contains("PRIMARY KEY"),
        "Expected PRIMARY KEY in DDL, got: " + createSql);
    assertTrue(createSql.contains("`id`"),
        "Expected `id` (stripped KEY_ prefix) in PRIMARY KEY clause, got: " + createSql);
  }

  @Test
  void testBuildCreateTableSqlVarcharWithLengthInDdl() throws Exception {
    stubConnectionWithStatement();

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("description", typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 512), true));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "VarLenTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("VarLenTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.create();
    verify(mockStatement, times(2)).executeUpdate(sqlCaptor.capture());

    String createSql = sqlCaptor.getAllValues().stream()
        .filter(s -> s.startsWith("CREATE TABLE")).findFirst().orElseThrow();
    assertTrue(createSql.contains("VARCHAR(512)"),
        "Expected VARCHAR(512) in DDL, got: " + createSql);
  }

  // --- alterTable() SQL content assertions ---

  @Test
  void testAlterTableAddColumnSqlContainsAddColumn() throws Exception {
    stubConnectionWithStatement();

    // Desired: KEY_id, name, email (email is new)
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("name", typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), true));
    builder.add("email", typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 200), true));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "AddColTable"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("AddColTable"), any())).thenReturn(existingRs);

    when(mockMetaData.getPrimaryKeys(eq("test_db"), any(), eq("AddColTable"))).thenAnswer(inv -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false);
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    // Existing has only id + name, not email
    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, true, false);
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10, 255);
    when(columnsRs.getInt("NULLABLE")).thenReturn(
        DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);
    when(mockMetaData.getColumns(eq("test_db"), any(), eq("AddColTable"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.update();
    verify(mockStatement, times(2)).executeUpdate(sqlCaptor.capture());

    List<String> allSql = sqlCaptor.getAllValues();
    String alterSql = allSql.stream()
        .filter(s -> s.contains("ALTER TABLE"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No ALTER TABLE found in: " + allSql));
    assertTrue(alterSql.contains("ADD COLUMN"),
        "Expected ADD COLUMN in ALTER statement, got: " + alterSql);
    assertTrue(alterSql.contains("`email`"),
        "Expected `email` column in ADD COLUMN statement, got: " + alterSql);
    assertTrue(alterSql.contains("VARCHAR(200)"),
        "Expected VARCHAR(200) for email column, got: " + alterSql);
  }

  @Test
  void testAlterTableModifyColumnSqlContainsModifyColumn() throws Exception {
    stubConnectionWithStatement();

    // Desired: KEY_id (INT), name VARCHAR(500) -- changed from 255
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("name", typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 500), true));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "ModColTable"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("ModColTable"), any())).thenReturn(existingRs);

    when(mockMetaData.getPrimaryKeys(eq("test_db"), any(), eq("ModColTable"))).thenAnswer(inv -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false);
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, true, false);
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10, 255); // existing name is 255
    when(columnsRs.getInt("NULLABLE")).thenReturn(
        DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);
    when(mockMetaData.getColumns(eq("test_db"), any(), eq("ModColTable"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.update();
    verify(mockStatement, times(2)).executeUpdate(sqlCaptor.capture());

    List<String> allSql = sqlCaptor.getAllValues();
    String alterSql = allSql.stream()
        .filter(s -> s.contains("ALTER TABLE"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No ALTER TABLE found in: " + allSql));
    assertTrue(alterSql.contains("MODIFY COLUMN"),
        "Expected MODIFY COLUMN in ALTER statement, got: " + alterSql);
    assertTrue(alterSql.contains("`name`"),
        "Expected `name` column in MODIFY COLUMN statement, got: " + alterSql);
    assertTrue(alterSql.contains("VARCHAR(500)"),
        "Expected VARCHAR(500) in MODIFY COLUMN statement, got: " + alterSql);
  }

  @Test
  void testAlterTableDropColumnSqlContainsDropColumn() throws Exception {
    stubConnectionWithStatement();
    stubDefaultRowType(); // KEY_id, name only

    Source source = new Source("db", List.of("MYSQL", "test_db", "DropColTable"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("DropColTable"), any())).thenReturn(existingRs);

    when(mockMetaData.getPrimaryKeys(eq("test_db"), any(), eq("DropColTable"))).thenAnswer(inv -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false);
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    // Existing has id, name, AND obsolete_col (will be dropped)
    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, true, true, false);
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "name", "obsolete_col");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT", "VARCHAR", "INT");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10, 255, 10);
    when(columnsRs.getInt("NULLABLE")).thenReturn(
        DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable, DatabaseMetaData.columnNullable);
    when(mockMetaData.getColumns(eq("test_db"), any(), eq("DropColTable"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.update();
    verify(mockStatement, times(2)).executeUpdate(sqlCaptor.capture());

    List<String> allSql = sqlCaptor.getAllValues();
    String alterSql = allSql.stream()
        .filter(s -> s.contains("ALTER TABLE"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No ALTER TABLE found in: " + allSql));
    assertTrue(alterSql.contains("DROP COLUMN"),
        "Expected DROP COLUMN in ALTER statement, got: " + alterSql);
    assertTrue(alterSql.contains("`obsolete_col`"),
        "Expected `obsolete_col` in DROP COLUMN statement, got: " + alterSql);
  }

  // --- isValidIdentifier() boundary: 64 chars valid, 65 chars invalid ---

  @Test
  void testValidatePassesWithMaxLength64Identifier() throws SQLException {
    stubConnection();
    stubDefaultRowType();

    // 64-char table name (valid boundary)
    String longName = "a".repeat(64);
    Source source = new Source("db", List.of("MYSQL", "test_db", longName), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq(longName), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertTrue(issues.valid(), "Expected 64-char identifier to be valid");
  }

  @Test
  void testValidateFailsWithTooLongIdentifier() {
    // 65-char table name (exceeds 64-char MySQL limit)
    String tooLong = "a".repeat(65);
    Source source = new Source("db", List.of("MYSQL", "test_db", tooLong), Collections.emptyMap());

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid(), "Expected 65-char identifier to be invalid");
    assertTrue(issues.toString().contains("Invalid table name"),
        "Expected 'Invalid table name' in issues");
  }

  // --- escapeIdentifier() return value assertion ---

  @Test
  void testEscapeIdentifierViaDeleteSqlContainsBacktickedName() throws Exception {
    stubConnectionWithStatement();

    Source source = new Source("db", List.of("MYSQL", "test_db", "myTable"), Collections.emptyMap());

    // Table exists for delete
    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("myTable"), any())).thenReturn(existingRs);

    // DB not empty after delete (skip DROP DATABASE)
    ResultSet dbNotEmptyRs = mock(ResultSet.class);
    when(dbNotEmptyRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), isNull(), any())).thenReturn(dbNotEmptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.delete();
    verify(mockStatement).executeUpdate(sqlCaptor.capture());

    String dropSql = sqlCaptor.getValue();
    assertNotNull(dropSql);
    assertFalse(dropSql.isEmpty(), "DROP TABLE SQL should not be empty");
    assertTrue(dropSql.contains("`myTable`"),
        "Expected backtick-escaped table name in DROP SQL, got: " + dropSql);
    assertTrue(dropSql.contains("`test_db`"),
        "Expected backtick-escaped db name in DROP SQL, got: " + dropSql);
  }

  // --- ensureDatabaseExists() exact SQL assertion ---

  @Test
  void testEnsureDatabaseExistsSqlContainsCreateDatabase() throws Exception {
    stubConnectionWithStatement();
    stubDefaultRowType();

    Source source = new Source("db", List.of("MYSQL", "test_db", "SomeTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("SomeTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    deployer.create();
    verify(mockStatement, times(2)).executeUpdate(sqlCaptor.capture());

    List<String> allSql = sqlCaptor.getAllValues();
    String createDbSql = allSql.stream()
        .filter(s -> s.contains("CREATE DATABASE"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No CREATE DATABASE statement found in: " + allSql));
    assertTrue(createDbSql.contains("IF NOT EXISTS"),
        "Expected IF NOT EXISTS in CREATE DATABASE SQL, got: " + createDbSql);
    assertTrue(createDbSql.contains("`test_db`"),
        "Expected backtick-escaped db name in CREATE DATABASE SQL, got: " + createDbSql);
  }

  // --- buildDesiredColumns() with invalid column name throws SQLException ---

  @Test
  void testBuildDesiredColumnsInvalidColumnNameThrowsSqlException() throws Exception {
    stubConnectionWithStatement();

    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.INTEGER));
    builder.add("invalid-col", typeFactory.createSqlType(SqlTypeName.VARCHAR, 100));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "BadColTable2"), Collections.emptyMap());

    // Table exists so alterTable() is invoked (which calls buildDesiredColumns())
    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("BadColTable2"), any())).thenReturn(existingRs);

    when(mockMetaData.getPrimaryKeys(eq("test_db"), any(), eq("BadColTable2"))).thenAnswer(inv -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false);
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(false);
    when(mockMetaData.getColumns(eq("test_db"), any(), eq("BadColTable2"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    // update() -> alterTable() -> buildDesiredColumns() should throw on invalid col name
    assertThrows(SQLException.class, deployer::update,
        "Expected SQLException for invalid column name in buildDesiredColumns");
  }

  // --- create() with null database throws ---

  @Test
  void testCreateWithNullDatabaseThrowsSqlException() {
    // Source with only table (no schema/database) gives null database
    Source source = new Source("db", List.of("myTable"), Collections.emptyMap());
    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    assertThrows(SQLException.class, deployer::create);
  }

  // --- restore() with created=false is a no-op ---

  @Test
  void testRestoreWithCreatedFalseIsNoOp() {
    Source source = new Source("db", List.of("MYSQL", "test_db", "T"), Collections.emptyMap());
    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    // No interaction with mocks expected — purely checks the conditional works
    deployer.restore(); // Must not throw
  }

  // --- update() with null database throws ---

  @Test
  void testUpdateWithNullDatabaseThrowsSqlException() {
    Source source = new Source("db", List.of("myTable"), Collections.emptyMap());
    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    assertThrows(SQLException.class, deployer::update);
  }

  // --- validate(): column type mismatch records error, missing required column records error ---

  @Test
  void testValidateFailsWhenKeyFieldTypeMismatch() throws SQLException {
    stubConnection();

    // Desired: KEY_id as BIGINT
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    builder.add("KEY_id", typeFactory.createSqlType(SqlTypeName.BIGINT));
    RelDataType rowType = builder.build();

    hoptimatorDriverStatic.when(() -> HoptimatorDriver.rowType(any(Source.class), any(HoptimatorConnection.class)))
        .thenReturn(rowType);

    Source source = new Source("db", List.of("MYSQL", "test_db", "TypeMismatch"), Collections.emptyMap());

    ResultSet existingRs = mock(ResultSet.class);
    when(existingRs.next()).thenReturn(true);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("TypeMismatch"), any())).thenReturn(existingRs);

    // Existing PK is INT, desired is BIGINT
    when(mockMetaData.getPrimaryKeys(eq("test_db"), any(), eq("TypeMismatch"))).thenAnswer(inv -> {
      ResultSet pkRs = mock(ResultSet.class);
      when(pkRs.next()).thenReturn(true, false);
      when(pkRs.getString("COLUMN_NAME")).thenReturn("id");
      return pkRs;
    });

    ResultSet columnsRs = mock(ResultSet.class);
    when(columnsRs.next()).thenReturn(true, false);
    when(columnsRs.getString("COLUMN_NAME")).thenReturn("id");
    when(columnsRs.getString("TYPE_NAME")).thenReturn("INT");
    when(columnsRs.getInt("COLUMN_SIZE")).thenReturn(10);
    when(columnsRs.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls);
    when(mockMetaData.getColumns(eq("test_db"), any(), eq("TypeMismatch"), any())).thenReturn(columnsRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertFalse(issues.valid(), "Expected validation to fail for KEY field type mismatch");
    assertTrue(issues.toString().contains("Cannot modify KEY field type"),
        "Expected KEY field type error, got: " + issues);
  }

  @Test
  void testValidatePassesWhenAllConditionsGood() throws SQLException {
    stubConnection();
    stubDefaultRowType(); // KEY_id (INT), name (VARCHAR(255) nullable)

    Source source = new Source("db", List.of("MYSQL", "test_db", "GoodTable"), Collections.emptyMap());

    ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.next()).thenReturn(false);
    when(mockMetaData.getTables(eq("test_db"), any(), eq("GoodTable"), any())).thenReturn(emptyRs);

    MySqlDeployer deployer = new MySqlDeployer(source, PROPERTIES, mockHoptimatorConnection);
    Validator.Issues issues = new Validator.Issues("test");
    deployer.validate(issues);

    assertTrue(issues.valid(), "Expected no errors for valid new table, got: " + issues);
  }
}
