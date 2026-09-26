package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.avro.HoptimatorTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
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
import java.sql.Types;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class MySqlTableTest {

  private static final String DATABASE = "test_db";
  private static final String TABLE = "test_table";

  @Mock
  private Connection mockConnection;

  @Mock
  private DatabaseMetaData mockMetaData;

  @Mock
  private ResultSet mockResultSet;

  @Mock
  private MockedStatic<DriverManager> driverManagerStatic;

  private Properties properties;
  private RelDataTypeFactory typeFactory;

  @BeforeEach
  void setUp() throws SQLException {
    properties = new Properties();
    properties.setProperty("url", "jdbc:mysql://localhost:3306/test");
    properties.setProperty("user", "testuser");
    properties.setProperty("password", "testpass");

    typeFactory = new SqlTypeFactoryImpl(HoptimatorTypeSystem.INSTANCE);
  }

  private void stubSuccessfulConnection() throws SQLException {
    when(mockConnection.getMetaData()).thenReturn(mockMetaData);
    driverManagerStatic.when(() -> {
      try (Connection c = DriverManager.getConnection(anyString(), anyString(), anyString())) {
        assert true; // recording-only
      }
    }).thenReturn(mockConnection);
  }

  @Test
  void testGetRowTypeWithColumns() throws SQLException {
    stubSuccessfulConnection();
    when(mockMetaData.getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(mockResultSet.getInt("DATA_TYPE")).thenReturn(Types.INTEGER, Types.VARCHAR);
    when(mockResultSet.getInt("NULLABLE"))
        .thenReturn(DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
    assertEquals("id", rowType.getFieldList().get(0).getName());
    assertEquals("name", rowType.getFieldList().get(1).getName());
  }

  @Test
  void testGetRowTypeReturnsFallbackOnSqlException() throws SQLException {
    // Stub getMetaData to throw, simulating a connection issue after connect
    stubSuccessfulConnection();
    // Use doThrow to override the previous stubbing
    doThrow(new SQLException("Metadata failed")).when(mockMetaData)
        .getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull());

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    assertNotNull(rowType);
    assertEquals(1, rowType.getFieldCount());
    assertEquals("ERROR", rowType.getFieldList().get(0).getName());
    assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testGetRowTypeWithEmptyTable() throws SQLException {
    stubSuccessfulConnection();
    when(mockMetaData.getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    assertNotNull(rowType);
    assertEquals(0, rowType.getFieldCount());
  }

  static Stream<Arguments> jdbcTypeToSqlTypeCases() {
    return Stream.of(
        Arguments.of("CHAR", Types.CHAR, SqlTypeName.VARCHAR),
        Arguments.of("VARCHAR", Types.VARCHAR, SqlTypeName.VARCHAR),
        Arguments.of("LONGVARCHAR", Types.LONGVARCHAR, SqlTypeName.VARCHAR),
        Arguments.of("NUMERIC", Types.NUMERIC, SqlTypeName.DECIMAL),
        Arguments.of("DECIMAL", Types.DECIMAL, SqlTypeName.DECIMAL),
        Arguments.of("BIT", Types.BIT, SqlTypeName.BOOLEAN),
        Arguments.of("BOOLEAN", Types.BOOLEAN, SqlTypeName.BOOLEAN),
        Arguments.of("TINYINT", Types.TINYINT, SqlTypeName.TINYINT),
        Arguments.of("SMALLINT", Types.SMALLINT, SqlTypeName.SMALLINT),
        Arguments.of("INTEGER", Types.INTEGER, SqlTypeName.INTEGER),
        Arguments.of("BIGINT", Types.BIGINT, SqlTypeName.BIGINT),
        Arguments.of("REAL", Types.REAL, SqlTypeName.REAL),
        Arguments.of("FLOAT", Types.FLOAT, SqlTypeName.DOUBLE),
        Arguments.of("DOUBLE", Types.DOUBLE, SqlTypeName.DOUBLE),
        Arguments.of("BINARY", Types.BINARY, SqlTypeName.VARBINARY),
        Arguments.of("VARBINARY", Types.VARBINARY, SqlTypeName.VARBINARY),
        Arguments.of("LONGVARBINARY", Types.LONGVARBINARY, SqlTypeName.VARBINARY),
        Arguments.of("DATE", Types.DATE, SqlTypeName.DATE),
        Arguments.of("TIME", Types.TIME, SqlTypeName.TIME),
        Arguments.of("TIMESTAMP", Types.TIMESTAMP, SqlTypeName.TIMESTAMP),
        Arguments.of("UNKNOWN_TYPE", Types.OTHER, SqlTypeName.VARCHAR)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("jdbcTypeToSqlTypeCases")
  void testJdbcTypeMapping(String name, int jdbcType, SqlTypeName expectedSqlType) throws SQLException {
    stubSuccessfulConnection();
    when(mockMetaData.getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("COLUMN_NAME")).thenReturn("col");
    when(mockResultSet.getInt("DATA_TYPE")).thenReturn(jdbcType);
    when(mockResultSet.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls);

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    assertEquals(expectedSqlType, rowType.getFieldList().get(0).getType().getSqlTypeName());
  }

  @Test
  void testGetRowTypeWithNullableColumn() throws SQLException {
    stubSuccessfulConnection();
    when(mockMetaData.getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("COLUMN_NAME")).thenReturn("nullable_col");
    when(mockResultSet.getInt("DATA_TYPE")).thenReturn(Types.INTEGER);
    when(mockResultSet.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNullable);

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    assertNotNull(rowType);
    assertTrue(rowType.getFieldList().get(0).getType().isNullable());
  }

  // --- nullable vs non-nullable column → different isNullable() result ---

  @Test
  void testGetRowTypeNonNullableColumnIsNotNullable() throws SQLException {
    stubSuccessfulConnection();
    when(mockMetaData.getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("COLUMN_NAME")).thenReturn("required_col");
    when(mockResultSet.getInt("DATA_TYPE")).thenReturn(Types.INTEGER);
    when(mockResultSet.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls);

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    assertNotNull(rowType);
    assertFalse(rowType.getFieldList().get(0).getType().isNullable(),
        "Non-nullable column (columnNoNulls) should not be nullable");
  }

  @Test
  void testGetRowTypeNullableAndNonNullableColumnsDiffer() throws SQLException {
    stubSuccessfulConnection();
    when(mockMetaData.getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString("COLUMN_NAME")).thenReturn("id", "name");
    when(mockResultSet.getInt("DATA_TYPE")).thenReturn(Types.INTEGER, Types.VARCHAR);
    when(mockResultSet.getInt("NULLABLE")).thenReturn(
        DatabaseMetaData.columnNoNulls, DatabaseMetaData.columnNullable);

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    assertNotNull(rowType);
    assertEquals(2, rowType.getFieldCount());
    assertFalse(rowType.getFieldList().get(0).getType().isNullable(),
        "id column (columnNoNulls) should not be nullable");
    assertTrue(rowType.getFieldList().get(1).getType().isNullable(),
        "name column (columnNullable) should be nullable");
  }

  // --- temporal fractional-seconds precision from JDBC DECIMAL_DIGITS ---

  @Test
  void testTimestampColumnCarriesMillisPrecision() throws SQLException {
    stubSuccessfulConnection();
    when(mockMetaData.getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("COLUMN_NAME")).thenReturn("created_at");
    when(mockResultSet.getInt("DATA_TYPE")).thenReturn(Types.TIMESTAMP);
    when(mockResultSet.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls);
    when(mockResultSet.getInt("DECIMAL_DIGITS")).thenReturn(3);
    when(mockResultSet.wasNull()).thenReturn(false);

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    RelDataType type = rowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.TIMESTAMP, type.getSqlTypeName());
    assertEquals(3, type.getPrecision());
  }

  @Test
  void testTimestampColumnCarriesMicrosPrecision() throws SQLException {
    stubSuccessfulConnection();
    when(mockMetaData.getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("COLUMN_NAME")).thenReturn("created_at");
    when(mockResultSet.getInt("DATA_TYPE")).thenReturn(Types.TIMESTAMP);
    when(mockResultSet.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls);
    when(mockResultSet.getInt("DECIMAL_DIGITS")).thenReturn(6);
    when(mockResultSet.wasNull()).thenReturn(false);

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    RelDataType type = rowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.TIMESTAMP, type.getSqlTypeName());
    assertEquals(6, type.getPrecision());
  }

  @Test
  void testTimestampColumnWithoutFractionalStaysDefaultPrecision() throws SQLException {
    stubSuccessfulConnection();
    when(mockMetaData.getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("COLUMN_NAME")).thenReturn("event_time");
    when(mockResultSet.getInt("DATA_TYPE")).thenReturn(Types.TIMESTAMP);
    when(mockResultSet.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls);
    when(mockResultSet.getInt("DECIMAL_DIGITS")).thenReturn(0);

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    RelDataType type = rowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.TIMESTAMP, type.getSqlTypeName());
    assertEquals(0, type.getPrecision());
  }

  @Test
  void testTimeColumnCarriesFractionalPrecision() throws SQLException {
    stubSuccessfulConnection();
    when(mockMetaData.getColumns(eq(DATABASE), isNull(), eq(TABLE), isNull())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getString("COLUMN_NAME")).thenReturn("event_time");
    when(mockResultSet.getInt("DATA_TYPE")).thenReturn(Types.TIME);
    when(mockResultSet.getInt("NULLABLE")).thenReturn(DatabaseMetaData.columnNoNulls);
    when(mockResultSet.getInt("DECIMAL_DIGITS")).thenReturn(3);
    when(mockResultSet.wasNull()).thenReturn(false);

    MySqlTable table = new MySqlTable(DATABASE, TABLE, properties);
    RelDataType rowType = table.getRowType(typeFactory);

    RelDataType type = rowType.getFieldList().get(0).getType();
    assertEquals(SqlTypeName.TIME, type.getSqlTypeName());
    assertEquals(3, type.getPrecision());
  }

}
