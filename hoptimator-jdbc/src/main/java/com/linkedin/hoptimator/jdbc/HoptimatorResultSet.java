package com.linkedin.hoptimator.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.util.Pair;


class HoptimatorResultSet implements ResultSet {
  private List<List<Object>> rows;
  private final List<Pair<String, Integer>> columnList;
  private int cursor = 0; // 1-based; 0 is _before_ first row
  private boolean wasNull = false;
  private boolean closed = false;
  private int fetchSize = 100;

  HoptimatorResultSet(List<List<Object>> rows,
      List<Pair<String, Integer>> columnList) {
    this.rows = rows;
    this.columnList = columnList;
  }

  @Override
  public synchronized boolean next() throws SQLException {
    checkClosed();

    cursor++;
    return cursor <= rows.size();
  }

  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("Connection has been closed.");
    }
  }

  private Object value(int j) {
    List<Object> row = rows.get(cursor - 1);
    if (j < 1 || j > row.size()) {
      return null;
    }
    wasNull = row.get(j - 1) == null;
    return row.get(j - 1);
  }

  @Override
  public synchronized void close() throws SQLException {
    closed = true;
  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  @Override
  public String getString(int j) throws SQLException {
    Object obj = value(j);
    return null == obj ? null : obj.toString();
  }

  @Override
  public boolean getBoolean(int j) throws SQLException {
    Boolean o = (Boolean) value(j);
    return o != null && o;
  }

  @Override
  public byte getByte(int j) throws SQLException {
    Object obj = value(j);
    if (null == obj) {
      return 0;
    } else if (obj instanceof Integer) {
      return ((Integer) obj).byteValue();
    }
    return (Byte) obj;
  }

  @Override
  public short getShort(int j) throws SQLException {
    Object obj = value(j);
    if (null == obj) {
      return 0;
    } else if (obj instanceof Integer) {
      return ((Integer) obj).shortValue();
    }
    return (Short) obj;
  }

  @Override
  public int getInt(int j) throws SQLException {
    Integer o = (Integer) value(j);
    return o == null ? 0 : o;
  }

  @Override
  public long getLong(int j) throws SQLException {
    Long o = (Long) value(j);
    return o == null ? 0 : o;
  }

  @Override
  public float getFloat(int j) throws SQLException {
    Float o = (Float) value(j);
    return o == null ? 0f : o;
  }

  @Override
  public double getDouble(int j) throws SQLException {
    Object obj = value(j);
    if (null == obj) {
      return 0d;
    } else if (obj instanceof BigDecimal) {
      return ((BigDecimal) obj).doubleValue();
    }
    return (Double) obj;
  }

  @Deprecated
  @Override
  public BigDecimal getBigDecimal(int j, int scale) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public byte[] getBytes(int j) throws SQLException {
    Object obj = value(j);
    if (null == obj) {
      return null;
    }
    if (obj instanceof ByteString) {
      return ((ByteString) obj).getBytes();
    } else if (obj instanceof String) {
      // Need to unwind the base64 for JSON
      return ByteString.parseBase64((String) obj);
    } else if (obj instanceof byte[]) {
      // Protobuf would have a byte array
      return (byte[]) obj;
    } else {
      throw new RuntimeException("Cannot handle " + obj.getClass() + " as bytes");
    }
  }

  @Override
  public Date getDate(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Time getTime(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Timestamp getTimestamp(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getAsciiStream(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Deprecated
  @Override
  public InputStream getUnicodeStream(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getBinaryStream(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getString(String s) throws SQLException {
    return getString(findColumn(s));
  }

  @Override
  public boolean getBoolean(String s) throws SQLException {
    return getBoolean(findColumn(s));
  }

  @Override
  public byte getByte(String s) throws SQLException {
    return getByte(findColumn(s));
  }

  @Override
  public short getShort(String s) throws SQLException {
    return getShort(findColumn(s));
  }

  @Override
  public int getInt(String s) throws SQLException {
    return getInt(findColumn(s));
  }

  @Override
  public long getLong(String s) throws SQLException {
    return getLong(findColumn(s));
  }

  @Override
  public float getFloat(String s) throws SQLException {
    return getFloat(findColumn(s));
  }

  @Override
  public double getDouble(String s) throws SQLException {
    return getDouble(findColumn(s));
  }

  @Deprecated
  @Override
  public BigDecimal getBigDecimal(String s, int scale) throws SQLException {
    return getBigDecimal(findColumn(s), scale);
  }

  @Override
  public byte[] getBytes(String s) throws SQLException {
    return getBytes(findColumn(s));
  }

  @Override
  public Date getDate(String s) throws SQLException {
    return getDate(findColumn(s));
  }

  @Override
  public Time getTime(String s) throws SQLException {
    return getTime(findColumn(s));
  }

  @Override
  public Timestamp getTimestamp(String s) throws SQLException {
    return getTimestamp(findColumn(s));
  }

  @Override
  public InputStream getAsciiStream(String s) throws SQLException {
    return getAsciiStream(findColumn(s));
  }

  @Deprecated
  @Override
  public InputStream getUnicodeStream(String s) throws SQLException {
    return getUnicodeStream(findColumn(s));
  }

  @Override
  public InputStream getBinaryStream(String s) throws SQLException {
    return getBinaryStream(findColumn(s));
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
  }

  @Override
  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return new HoptimatorResultSetMetaData();
  }

  @Override
  public Object getObject(int j) throws SQLException {
    return value(j);
  }

  @Override
  public Object getObject(String s) throws SQLException {
    return getObject(findColumn(s));
  }

  @Override
  public synchronized int findColumn(String s) throws SQLException {
    // index is 1-based
    for (int i = 1; i <= columnList.size(); i++) {
      if (columnList.get(i - 1).left.equals(s)) {
        return i;
      }
    }
    throw new SQLException("No column '" + s + "' found.");
  }

  @Override
  public Reader getCharacterStream(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getCharacterStream(String s) throws SQLException {
    return getCharacterStream(findColumn(s));
  }

  @Override
  public BigDecimal getBigDecimal(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public BigDecimal getBigDecimal(String s) throws SQLException {
    return getBigDecimal(findColumn(s));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    return cursor == 0;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    return cursor > rows.size();
  }

  @Override
  public boolean isFirst() throws SQLException {
    return cursor == 1;
  }

  @Override
  public boolean isLast() throws SQLException {
    return cursor == rows.size();
  }

  @Override
  public void beforeFirst() throws SQLException {
    cursor = 0;
  }

  @Override
  public void afterLast() throws SQLException {
    cursor = Integer.MAX_VALUE;
  }

  @Override
  public boolean first() throws SQLException {
    cursor = 1;
    return isFirst();
  }

  @Override
  public synchronized boolean last() throws SQLException {
    cursor = rows.size();
    return isLast();
  }

  @Override
  public int getRow() throws SQLException {
    if (isAfterLast()) {
      return 0;
    } else {
      return cursor;
    }
  }

  @Override
  public synchronized boolean absolute(int row) throws SQLException {
    cursor = row;
    return cursor <= rows.size();
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    return absolute(cursor + rows);
  }

  @Override
  public boolean previous() throws SQLException {
    cursor--;
    return cursor > 0;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    if (rows < 0) {
      throw new IllegalArgumentException("Fetch size must not be negative");
    }
    fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  @Override
  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    return false;
  }

  @Override
  public void updateNull(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(int j, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(int j, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(int j, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(int j, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(int j, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(int j, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(int j, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(int j, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(int j, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(int j, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(int j, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(int j, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(int j, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int j, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int j, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int j, Reader x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int j, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int j, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(String s) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(String s, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(String s, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(String s, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(String s, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(String s, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(String s, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(String s, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(String s, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(String s, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(String s, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(String s, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(String s, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(String s, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String s, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String s, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String s, Reader reader, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String s, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String s, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement getStatement() throws SQLException {
    return null;
  }

  @Override
  public Object getObject(int j, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
    return getObject(findColumn(s), map);
  }

  @Override
  public Ref getRef(String s) throws SQLException {
    return getRef(findColumn(s));
  }

  @Override
  public Blob getBlob(String s) throws SQLException {
    return getBlob(findColumn(s));
  }

  @Override
  public Clob getClob(String s) throws SQLException {
    return getClob(findColumn(s));
  }

  @Override
  public Array getArray(String s) throws SQLException {
    return getArray(findColumn(s));
  }

  @Override
  public Date getDate(int j, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(String s, Calendar cal) throws SQLException {
    return getDate(findColumn(s), cal);
  }

  @Override
  public Time getTime(int j, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Time getTime(String s, Calendar cal) throws SQLException {
    return getTime(findColumn(s), cal);
  }

  @Override
  public Timestamp getTimestamp(int j, Calendar cal) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Timestamp getTimestamp(String s, Calendar cal) throws SQLException {
    return getTimestamp(findColumn(s), cal);
  }

  @Override
  public URL getURL(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public URL getURL(String s) throws SQLException {
    return getURL(findColumn(s));
  }

  @Override
  public void updateRef(int j, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(String s, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int j, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String s, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int j, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String s, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(int j, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(String s, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(String s) throws SQLException {
    return getRowId(findColumn(s));
  }

  @Override
  public void updateRowId(int j, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(String s, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed;
  }

  @Override
  public void updateNString(int j, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(String s, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int j, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String s, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(String s) throws SQLException {
    return getNClob(findColumn(s));
  }

  @Override
  public SQLXML getSQLXML(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(String s) throws SQLException {
    return getSQLXML(findColumn(s));
  }

  @Override
  public void updateSQLXML(int j, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(String s, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(String s) throws SQLException {
    return getNString(findColumn(s));
  }

  @Override
  public Reader getNCharacterStream(int j) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(String s) throws SQLException {
    return getNCharacterStream(findColumn(s));
  }

  @Override
  public void updateNCharacterStream(int j, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String s, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int j, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int j, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int j, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String s, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String s, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String s, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int j, InputStream inputStream, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String s, InputStream inputStream, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int j, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String s, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int j, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String s, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int j, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String s, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int j, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int j, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int j, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String s, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String s, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String s, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int j, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String s, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int j, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String s, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int j, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String s, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T getObject(int j, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T getObject(String s, Class<T> type) throws SQLException {
    return getObject(findColumn(s), type);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  private final class HoptimatorResultSetMetaData implements ResultSetMetaData {

    @Override
    public int getColumnCount() throws SQLException {
      return columnList.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
      return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
      return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int isNullable(int j) throws SQLException {
      return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int j) throws SQLException {
      return false;
    }

    @Override
    public int getColumnDisplaySize(int j) throws SQLException {
      return 20;
    }

    @Override
    public String getColumnLabel(int j) throws SQLException {
      return columnList.get(j - 1).left;
    }

    @Override
    public String getColumnName(int j) throws SQLException {
      return columnList.get(j - 1).left;
    }

    @Override
    public String getSchemaName(int j) throws SQLException {
      return "";
    }

    @Override
    public int getPrecision(int j) throws SQLException {
      return 0;
    }

    @Override
    public int getScale(int j) throws SQLException {
      return 0;
    }

    @Override
    public String getTableName(int j) throws SQLException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getCatalogName(int j) throws SQLException {
      return "";
    }

    @Override
    public int getColumnType(int j) throws SQLException {
      return columnList.get(j - 1).right;
    }

    @Override
    public String getColumnTypeName(int j) throws SQLException {
      Object obj = value(j);
      return null == obj ? null : obj.getClass().getTypeName();
    }

    @Override
    public boolean isReadOnly(int j) throws SQLException {
      return true;
    }

    @Override
    public boolean isWritable(int j) throws SQLException {
      return false;
    }

    @Override
    public boolean isDefinitelyWritable(int j) throws SQLException {
      return false;
    }

    @Override
    public String getColumnClassName(int j) throws SQLException {
      Object obj = value(j);
      return null == obj ? null : obj.getClass().getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return false;
    }
  }
}
