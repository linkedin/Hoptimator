package com.linkedin.hoptimator.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class HoptimatorResultSetTest {

  private HoptimatorResultSet resultSet;

  @BeforeEach
  void setUp() {
    List<Pair<String, Integer>> columns = Arrays.asList(
        new Pair<>("NAME", Types.VARCHAR),
        new Pair<>("AGE", Types.INTEGER),
        new Pair<>("ACTIVE", Types.BOOLEAN)
    );
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList("Alice", 30, true));
    rows.add(Arrays.asList("Bob", null, false));
    resultSet = new HoptimatorResultSet(rows, columns);
  }

  @Test
  void testNextIteratesThroughRows() throws Exception {
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());
  }

  @Test
  void testGetStringByIndex() throws Exception {
    resultSet.next();

    assertEquals("Alice", resultSet.getString(1));
  }

  @Test
  void testGetStringByName() throws Exception {
    resultSet.next();

    assertEquals("Alice", resultSet.getString("NAME"));
  }

  @Test
  void testGetIntByIndex() throws Exception {
    resultSet.next();

    assertEquals(30, resultSet.getInt(2));
  }

  @Test
  void testGetIntReturnsZeroForNull() throws Exception {
    resultSet.next();
    resultSet.next();

    assertEquals(0, resultSet.getInt(2));
    assertTrue(resultSet.wasNull());
  }

  @Test
  void testGetBooleanByIndex() throws Exception {
    resultSet.next();

    assertTrue(resultSet.getBoolean(3));
  }

  @Test
  void testGetBooleanReturnsFalseForNull() throws Exception {
    resultSet.next();
    // set wasNull by reading a null
    resultSet.next();
    // Bob's ACTIVE is false
    assertFalse(resultSet.getBoolean(3));
  }

  @Test
  void testFindColumn() throws Exception {
    assertEquals(1, resultSet.findColumn("NAME"));
    assertEquals(2, resultSet.findColumn("AGE"));
    assertEquals(3, resultSet.findColumn("ACTIVE"));
  }

  @Test
  void testFindColumnThrowsForUnknown() {
    assertThrows(SQLException.class, () -> resultSet.findColumn("MISSING"));
  }

  @Test
  void testGetObjectByIndex() throws Exception {
    resultSet.next();

    assertEquals("Alice", resultSet.getObject(1));
    assertEquals(30, resultSet.getObject(2));
  }

  @Test
  void testGetObjectByName() throws Exception {
    resultSet.next();

    assertEquals("Alice", resultSet.getObject("NAME"));
  }

  @Test
  void testCloseAndIsClosed() throws Exception {
    assertFalse(resultSet.isClosed());

    resultSet.close();

    assertTrue(resultSet.isClosed());
  }

  @Test
  void testNextAfterCloseThrows() throws Exception {
    resultSet.close();

    assertThrows(IllegalStateException.class, () -> resultSet.next());
  }

  @Test
  void testIsBeforeFirst() throws Exception {
    assertTrue(resultSet.isBeforeFirst());

    resultSet.next();

    assertFalse(resultSet.isBeforeFirst());
  }

  @Test
  void testIsFirst() throws Exception {
    assertFalse(resultSet.isFirst());

    resultSet.next();

    assertTrue(resultSet.isFirst());
  }

  @Test
  void testIsLast() throws Exception {
    resultSet.next();
    assertFalse(resultSet.isLast());

    resultSet.next();
    assertTrue(resultSet.isLast());
  }

  @Test
  void testIsAfterLast() throws Exception {
    resultSet.next();
    resultSet.next();
    assertFalse(resultSet.isAfterLast());

    resultSet.next();
    assertTrue(resultSet.isAfterLast());
  }

  @Test
  void testGetRow() throws Exception {
    assertEquals(0, resultSet.getRow()); // before first: cursor=0, but isAfterLast is false so returns 0

    resultSet.next();
    assertEquals(1, resultSet.getRow());

    resultSet.next();
    assertEquals(2, resultSet.getRow());

    resultSet.next(); // after last
    assertEquals(0, resultSet.getRow());
  }

  @Test
  void testBeforeFirst() throws Exception {
    resultSet.next();
    resultSet.beforeFirst();

    assertTrue(resultSet.isBeforeFirst());
  }

  @Test
  void testAbsolute() throws Exception {
    assertTrue(resultSet.absolute(2));
    assertEquals("Bob", resultSet.getString(1));

    assertFalse(resultSet.absolute(3));
  }

  @Test
  void testRelative() throws Exception {
    resultSet.next(); // row 1
    assertTrue(resultSet.relative(1)); // row 2
    assertEquals("Bob", resultSet.getString(1));
  }

  @Test
  void testPrevious() throws Exception {
    resultSet.next();
    resultSet.next();

    assertTrue(resultSet.previous());
    assertEquals("Alice", resultSet.getString(1));
  }

  @Test
  void testFirst() throws Exception {
    resultSet.next();
    resultSet.next();

    assertTrue(resultSet.first());
    assertEquals("Alice", resultSet.getString(1));
  }

  @Test
  void testLast() throws Exception {
    assertTrue(resultSet.last());
    assertEquals("Bob", resultSet.getString(1));
  }

  @Test
  void testGetWarningsReturnsNull() throws Exception {
    assertNull(resultSet.getWarnings());
  }

  @Test
  void testClearWarningsIsNop() throws Exception {
    resultSet.clearWarnings();
  }

  @Test
  void testGetTypeReturnsForwardOnly() throws Exception {
    assertEquals(ResultSet.TYPE_FORWARD_ONLY, resultSet.getType());
  }

  @Test
  void testGetFetchDirectionReturnsForward() throws Exception {
    assertEquals(ResultSet.FETCH_FORWARD, resultSet.getFetchDirection());
  }

  @Test
  void testSetFetchDirectionForwardIsAllowed() throws Exception {
    resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Test
  void testSetFetchDirectionReverseThrows() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.setFetchDirection(ResultSet.FETCH_REVERSE));
  }

  @Test
  void testGetAndSetFetchSize() throws Exception {
    assertEquals(100, resultSet.getFetchSize());

    resultSet.setFetchSize(50);
    assertEquals(50, resultSet.getFetchSize());
  }

  @Test
  void testSetFetchSizeNegativeThrows() {
    assertThrows(IllegalArgumentException.class, () -> resultSet.setFetchSize(-1));
  }

  @Test
  void testRowUpdatedInsertedDeletedReturnFalse() throws Exception {
    assertFalse(resultSet.rowUpdated());
    assertFalse(resultSet.rowInserted());
    assertFalse(resultSet.rowDeleted());
  }

  @Test
  void testGetStatementReturnsNull() throws Exception {
    assertNull(resultSet.getStatement());
  }

  @Test
  void testUnwrapReturnsNull() throws Exception {
    assertNull(resultSet.unwrap(String.class));
  }

  @Test
  void testIsWrapperForReturnsFalse() throws Exception {
    assertFalse(resultSet.isWrapperFor(String.class));
  }

  @Test
  void testGetMetaData() throws Exception {
    ResultSetMetaData metaData = resultSet.getMetaData();

    assertEquals(3, metaData.getColumnCount());
    assertEquals("NAME", metaData.getColumnName(1));
    assertEquals("AGE", metaData.getColumnLabel(2));
    assertEquals(Types.INTEGER, metaData.getColumnType(2));
    assertEquals("VARCHAR", metaData.getColumnTypeName(1));
    assertTrue(metaData.isCaseSensitive(1));
    assertTrue(metaData.isSearchable(1));
    assertTrue(metaData.isReadOnly(1));
    assertFalse(metaData.isWritable(1));
    assertFalse(metaData.isDefinitelyWritable(1));
    assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(1));
    assertFalse(metaData.isSigned(1));
    assertEquals(20, metaData.getColumnDisplaySize(1));
    assertEquals("", metaData.getSchemaName(1));
    assertEquals("", metaData.getCatalogName(1));
    assertEquals(0, metaData.getPrecision(1));
    assertEquals(0, metaData.getScale(1));
  }

  @Test
  void testGetStringReturnsNullForOutOfBoundsIndex() throws Exception {
    resultSet.next();

    // column index 0 is out of bounds (< 1)
    assertNull(resultSet.getString(0));
    // column index 4 is out of bounds (> 3)
    assertNull(resultSet.getString(4));
  }

  @Test
  void testGetBytesWithNullReturnsNull() throws Exception {
    resultSet.next();
    resultSet.next(); // Bob row has null AGE

    assertNull(resultSet.getBytes(2));
  }

  @Test
  void testGetBytesWithByteArrayReturnsDirectly() throws Exception {
    byte[] data = new byte[]{1, 2, 3};
    List<Pair<String, Integer>> columns = List.of(
            new Pair<>("DATA", Types.VARBINARY)
    );
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(data));
    try (HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns)) {
      rs.next();

      byte[] result = rs.getBytes(1);

      assertArrayEquals(data, result);
    }
  }

  @Test
  void testGetByteByIndex() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.TINYINT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(42));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(42, rs.getByte(1));
  }

  @Test
  void testGetByteReturnsZeroForNull() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.TINYINT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList((Object) null));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(0, rs.getByte(1));
  }

  @Test
  void testGetByteByName() throws Exception {
    resultSet.next();

    assertEquals(30, resultSet.getByte("AGE"));
  }

  @Test
  void testGetShortByIndex() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.SMALLINT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(100));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(100, rs.getShort(1));
  }

  @Test
  void testGetShortReturnsZeroForNull() throws Exception {
    resultSet.next();
    resultSet.next();

    assertEquals(0, resultSet.getShort(2));
  }

  @Test
  void testGetShortByName() throws Exception {
    resultSet.next();

    assertEquals(30, resultSet.getShort("AGE"));
  }

  @Test
  void testGetLongByIndex() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.BIGINT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(999L));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(999L, rs.getLong(1));
  }

  @Test
  void testGetLongReturnsZeroForNull() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.BIGINT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList((Object) null));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(0L, rs.getLong(1));
  }

  @Test
  void testGetLongByName() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.BIGINT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(42L));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(42L, rs.getLong("VAL"));
  }

  @Test
  void testGetFloatByIndex() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.FLOAT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(1.5f));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(1.5f, rs.getFloat(1));
  }

  @Test
  void testGetFloatReturnsZeroForNull() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.FLOAT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList((Object) null));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(0f, rs.getFloat(1));
  }

  @Test
  void testGetFloatByName() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.FLOAT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(2.5f));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(2.5f, rs.getFloat("VAL"));
  }

  @Test
  void testGetDoubleByIndex() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.DOUBLE));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(1.5));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(1.5, rs.getDouble(1));
  }

  @Test
  void testGetDoubleReturnsZeroForNull() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.DOUBLE));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList((Object) null));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(0d, rs.getDouble(1));
  }

  @Test
  void testGetDoubleByName() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.DOUBLE));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(9.9));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(9.9, rs.getDouble("VAL"));
  }

  @Test
  void testGetDoubleWithBigDecimal() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.DECIMAL));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(new BigDecimal("1.5")));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertEquals(1.5, rs.getDouble(1));
  }

  @Test
  void testGetIntByName() throws Exception {
    resultSet.next();

    assertEquals(30, resultSet.getInt("AGE"));
  }

  @Test
  void testGetBooleanByName() throws Exception {
    resultSet.next();

    assertTrue(resultSet.getBoolean("ACTIVE"));
  }

  @Test
  void testGetBooleanReturnsFalseForNullValue() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("VAL", Types.BOOLEAN));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList((Object) null));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertFalse(rs.getBoolean(1));
  }

  @Test
  void testGetStringReturnsNullForNullValue() throws Exception {
    resultSet.next();
    resultSet.next();

    assertNull(resultSet.getString(2));
  }

  @Test
  void testAfterLast() throws Exception {
    resultSet.afterLast();

    assertTrue(resultSet.isAfterLast());
  }

  @Test
  void testGetBytesByName() throws Exception {
    byte[] data = new byte[]{1, 2, 3};
    List<Pair<String, Integer>> columns = List.of(new Pair<>("DATA", Types.VARBINARY));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(data));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertArrayEquals(data, rs.getBytes("DATA"));
  }

  @Test
  void testGetBytesWithStringBase64() throws Exception {
    String base64 = "AQID"; // base64 for {1, 2, 3}
    List<Pair<String, Integer>> columns = List.of(new Pair<>("DATA", Types.VARBINARY));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(base64));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    byte[] result = rs.getBytes(1);

    assertNotNull(result);
  }

  @Test
  void testGetBytesWithUnsupportedTypeThrows() throws Exception {
    List<Pair<String, Integer>> columns = List.of(new Pair<>("DATA", Types.VARBINARY));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(42));
    HoptimatorResultSet rs = new HoptimatorResultSet(rows, columns);
    rs.next();

    assertThrows(RuntimeException.class, () -> rs.getBytes(1));
  }

  // Test unsupported operations to cover the many throw lines
  @Test
  void testUnsupportedGetOperations() throws Exception {
    resultSet.next();

    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getDate(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTime(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTimestamp(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getAsciiStream(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBinaryStream(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getCursorName());
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getCharacterStream(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBigDecimal(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getConcurrency());
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRef(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBlob(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getClob(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getDate(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTime(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTimestamp(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getURL(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRowId(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getHoldability());
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNClob(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getSQLXML(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNString(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNCharacterStream(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getObject(1, String.class));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getObject(1, Map.of()));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBigDecimal(1, 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getUnicodeStream(1));
  }

  @Test
  void testUnsupportedGetByNameOperations() throws Exception {
    resultSet.next();

    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getDate("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTime("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTimestamp("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getAsciiStream("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBinaryStream("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getCharacterStream("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBigDecimal("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRef("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBlob("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getClob("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getArray("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getDate("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTime("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getTimestamp("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getURL("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getRowId("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNClob("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getSQLXML("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNString("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getNCharacterStream("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getObject("NAME", String.class));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getObject("NAME", Map.of()));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getBigDecimal("NAME", 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.getUnicodeStream("NAME"));
  }

  @Test
  void testUnsupportedUpdateByIndexOperations() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNull(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean(1, true));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateByte(1, (byte) 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateShort(1, (short) 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateInt(1, 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateLong(1, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateFloat(1, 0f));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDouble(1, 0d));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBigDecimal(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateString(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBytes(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDate(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTime(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTimestamp(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, null, 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, null, 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, null, 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(1, null, 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRef(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, (Blob) null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, (Clob) null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateArray(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRowId(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNString(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, (NClob) null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream(1, null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, (Reader) null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, (Reader) null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream(1, null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, (InputStream) null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, (Reader) null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, (Reader) null));
  }

  @Test
  void testUnsupportedUpdateByNameOperations() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNull("NAME"));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean("NAME", true));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateByte("NAME", (byte) 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateShort("NAME", (short) 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateInt("NAME", 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateLong("NAME", 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateFloat("NAME", 0f));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDouble("NAME", 0d));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBigDecimal("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateString("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBytes("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateDate("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTime("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateTimestamp("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("NAME", null, 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("NAME", null, 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("NAME", null, 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject("NAME", null, 0));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateObject("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRef("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob("NAME", (Blob) null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("NAME", (Clob) null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateArray("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRowId("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNString("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("NAME", (NClob) null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream("NAME", null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("NAME", null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("NAME", null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("NAME", null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob("NAME", null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("NAME", (Reader) null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("NAME", (Reader) null, 0L));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNCharacterStream("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateAsciiStream("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBinaryStream("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateCharacterStream("NAME", null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateBlob("NAME", (InputStream) null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateClob("NAME", (Reader) null));
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateNClob("NAME", (Reader) null));
  }

  @Test
  void testUnsupportedRowOperations() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.insertRow());
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.updateRow());
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.deleteRow());
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.refreshRow());
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.cancelRowUpdates());
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.moveToInsertRow());
    assertThrows(SQLFeatureNotSupportedException.class, () -> resultSet.moveToCurrentRow());
  }

  @Test
  void testGetMetaDataColumnClassName() throws Exception {
    resultSet.next();
    ResultSetMetaData metaData = resultSet.getMetaData();

    String className = metaData.getColumnClassName(1);

    assertEquals("java.lang.String", className);
  }

  @Test
  void testGetMetaDataColumnClassNameReturnsNullForNullValue() throws Exception {
    resultSet.next();
    resultSet.next(); // Bob row has null AGE
    ResultSetMetaData metaData = resultSet.getMetaData();

    String className = metaData.getColumnClassName(2);

    assertNull(className);
  }

  @Test
  void testGetMetaDataUnsupportedOperations() throws Exception {
    ResultSetMetaData metaData = resultSet.getMetaData();

    assertThrows(SQLFeatureNotSupportedException.class, () -> metaData.isAutoIncrement(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> metaData.isCurrency(1));
    assertThrows(SQLFeatureNotSupportedException.class, () -> metaData.getTableName(1));
  }

  @Test
  void testGetMetaDataUnwrapAndIsWrapperFor() throws Exception {
    ResultSetMetaData metaData = resultSet.getMetaData();

    assertNull(metaData.unwrap(String.class));
    assertFalse(metaData.isWrapperFor(String.class));
  }

  // --- By-name delegate methods that throw SQLFeatureNotSupportedException ---

  @Test
  void testGetBigDecimalWithScaleByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getBigDecimal("NAME", 2));
  }

  @Test
  void testGetDateByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getDate("NAME"));
  }

  @Test
  void testGetTimeByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getTime("NAME"));
  }

  @Test
  void testGetTimestampByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getTimestamp("NAME"));
  }

  @Test
  void testGetAsciiStreamByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getAsciiStream("NAME"));
  }

  @Test
  void testGetUnicodeStreamByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getUnicodeStream("NAME"));
  }

  @Test
  void testGetBinaryStreamByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getBinaryStream("NAME"));
  }

  @Test
  void testGetCharacterStreamByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getCharacterStream("NAME"));
  }

  @Test
  void testGetBigDecimalByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getBigDecimal("NAME"));
  }

  @Test
  void testGetObjectWithMapByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getObject("NAME", Collections.emptyMap()));
  }

  @Test
  void testGetRefByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getRef("NAME"));
  }

  @Test
  void testGetBlobByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getBlob("NAME"));
  }

  @Test
  void testGetClobByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getClob("NAME"));
  }

  @Test
  void testGetArrayByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getArray("NAME"));
  }

  @Test
  void testGetDateWithCalByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getDate("NAME", Calendar.getInstance()));
  }

  @Test
  void testGetTimeWithCalByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getTime("NAME", Calendar.getInstance()));
  }

  @Test
  void testGetTimestampWithCalByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getTimestamp("NAME", Calendar.getInstance()));
  }

  @Test
  void testGetURLByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getURL("NAME"));
  }

  @Test
  void testGetRowIdByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getRowId("NAME"));
  }

  @Test
  void testGetNClobByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getNClob("NAME"));
  }

  @Test
  void testGetSQLXMLByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getSQLXML("NAME"));
  }

  @Test
  void testGetNStringByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getNString("NAME"));
  }

  @Test
  void testGetNCharacterStreamByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getNCharacterStream("NAME"));
  }

  @Test
  void testGetObjectWithTypeByName() throws Exception {
    resultSet.next();
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> resultSet.getObject("NAME", String.class));
  }

  // --- Additional getByte/getShort edge cases ---

  @Test
  void testGetByteReturnsDirectByteValue() throws Exception {
    // Create a result set with actual Byte values
    List<Pair<String, Integer>> cols = Arrays.asList(new Pair<>("VAL", Types.TINYINT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList((Object) Byte.valueOf((byte) 42)));
    HoptimatorResultSet byteRs = new HoptimatorResultSet(rows, cols);
    byteRs.next();

    assertEquals((byte) 42, byteRs.getByte(1));
  }

  @Test
  void testGetShortReturnsDirectShortValue() throws Exception {
    List<Pair<String, Integer>> cols = Arrays.asList(new Pair<>("VAL", Types.SMALLINT));
    List<List<Object>> rows = new ArrayList<>();
    rows.add(Arrays.asList((Object) Short.valueOf((short) 1000)));
    HoptimatorResultSet shortRs = new HoptimatorResultSet(rows, cols);
    shortRs.next();

    assertEquals((short) 1000, shortRs.getShort(1));
  }
}
