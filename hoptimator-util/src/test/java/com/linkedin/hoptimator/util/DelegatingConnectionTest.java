package com.linkedin.hoptimator.util;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "assertThrows lambdas call methods that always throw — no resource is ever returned")
class DelegatingConnectionTest {

  @Mock
  private Connection mockInner;

  private DelegatingConnection connection;

  @BeforeEach
  void setUp() {
    connection = new DelegatingConnection(mockInner);
  }

  @Test
  void testGetAutoCommitAlwaysReturnsTrue() throws Exception {
    assertTrue(connection.getAutoCommit());
  }

  @Test
  void testSetAutoCommitIsNop() throws Exception {
    connection.setAutoCommit(false);
    // no exception, no delegation
    assertTrue(connection.getAutoCommit());
  }

  @Test
  void testGetTransactionIsolationReturnsNone() throws Exception {
    assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
  }

  @Test
  void testGetWarningsReturnsNull() throws Exception {
    assertNull(connection.getWarnings());
  }

  @Test
  void testCloseDelegate() throws Exception {
    connection.close();
    verify(mockInner).close();
  }

  @Test
  void testIsClosedDelegates() throws Exception {
    when(mockInner.isClosed()).thenReturn(true);
    assertTrue(connection.isClosed());
  }

  @Test
  void testGetSchemaDelegates() throws Exception {
    when(mockInner.getSchema()).thenReturn("public");
    assertEquals("public", connection.getSchema());
  }

  @Test
  void testSetSchemaDelegates() throws Exception {
    connection.setSchema("test");
    verify(mockInner).setSchema("test");
  }

  @Test
  void testCreateStatementReturnsDelegatingStatement() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      assertNotNull(stmt);
      assertTrue(stmt instanceof DelegatingStatement);
    }
  }

  @Test
  void testPrepareStatementThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> {
      try (PreparedStatement ps = connection.prepareStatement("SELECT 1")) {
        assertNotNull(ps);
      }
    });
  }

  @Test
  void testPrepareCallThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> {
      try (CallableStatement cs = connection.prepareCall("CALL proc()")) {
        assertNotNull(cs);
      }
    });
  }

  @Test
  void testClearWarningsThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.clearWarnings());
  }

  @Test
  void testUnwrapReturnsNull() throws Exception {
    assertNull(connection.unwrap(String.class));
  }

  @Test
  void testIsWrapperForReturnsFalse() throws Exception {
    assertFalse(connection.isWrapperFor(String.class));
  }

  @Test
  void testCommitIsNop() throws Exception {
    connection.commit();
    // no exception
  }

  @Test
  void testRollbackIsNop() throws Exception {
    connection.rollback();
    // no exception
    assertTrue(connection.getAutoCommit());
  }

  @Test
  void testSetTransactionIsolationIsNop() throws Exception {
    connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    assertEquals(Connection.TRANSACTION_NONE, connection.getTransactionIsolation());
  }

  @Test
  void testGetCatalogDelegates() throws Exception {
    when(mockInner.getCatalog()).thenReturn("myCatalog");
    assertEquals("myCatalog", connection.getCatalog());
  }

  @Test
  void testSetCatalogDelegates() throws Exception {
    connection.setCatalog("newCatalog");
    verify(mockInner).setCatalog("newCatalog");
  }

  @Test
  void testGetMetaDataDelegates() throws Exception {
    DatabaseMetaData mockMeta = mock(DatabaseMetaData.class);
    when(mockInner.getMetaData()).thenReturn(mockMeta);
    assertEquals(mockMeta, connection.getMetaData());
  }

  @Test
  void testSetReadOnlyDelegates() throws Exception {
    connection.setReadOnly(true);
    verify(mockInner).setReadOnly(true);
  }

  @Test
  void testIsReadOnlyDelegates() throws Exception {
    when(mockInner.isReadOnly()).thenReturn(true);
    assertTrue(connection.isReadOnly());
  }

  @Test
  void testNativeSqlThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.nativeSQL("SELECT 1"));
  }

  @Test
  void testCreateStatementWithParamsThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.createStatement(1, 2));
  }

  @Test
  void testCreateStatementWithThreeParamsThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.createStatement(1, 2, 3));
  }

  @Test
  void testPrepareStatementWithResultSetParamsThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.prepareStatement("SELECT 1", 1, 2));
  }

  @Test
  void testPrepareStatementWithHoldabilityThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.prepareStatement("SELECT 1", 1, 2, 3));
  }

  @Test
  void testPrepareStatementWithAutoGeneratedKeysThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.prepareStatement("SELECT 1", 1));
  }

  @Test
  void testPrepareStatementWithColumnIndexesThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.prepareStatement("SELECT 1", new int[]{1}));
  }

  @Test
  void testPrepareStatementWithColumnNamesThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.prepareStatement("SELECT 1", new String[]{"col1"}));
  }

  @Test
  void testPrepareCallWithParamsThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.prepareCall("CALL proc()", 1, 2));
  }

  @Test
  void testPrepareCallWithHoldabilityThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.prepareCall("CALL proc()", 1, 2, 3));
  }

  @Test
  void testGetTypeMapThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.getTypeMap());
  }

  @Test
  void testSetTypeMapThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setTypeMap(null));
  }

  @Test
  void testSetHoldabilityThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setHoldability(1));
  }

  @Test
  void testGetHoldabilityThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.getHoldability());
  }

  @Test
  void testSetSavepointThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setSavepoint());
  }

  @Test
  void testSetSavepointWithNameThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.setSavepoint("sp1"));
  }

  @Test
  void testRollbackSavepointThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.rollback(null));
  }

  @Test
  void testReleaseSavepointThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.releaseSavepoint(null));
  }

  @Test
  void testCreateClobThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.createClob());
  }

  @Test
  void testCreateBlobThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.createBlob());
  }

  @Test
  void testCreateNClobThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.createNClob());
  }

  @Test
  void testCreateSQLXMLThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.createSQLXML());
  }

  @Test
  void testIsValidThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.isValid(5));
  }

  @Test
  void testCreateArrayOfThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.createArrayOf("VARCHAR", new Object[]{}));
  }

  @Test
  void testCreateStructThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.createStruct("MY_TYPE", new Object[]{}));
  }

  @Test
  void testAbortThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.abort(null));
  }

  @Test
  void testSetNetworkTimeoutThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> connection.setNetworkTimeout(null, 1000));
  }

  @Test
  void testGetNetworkTimeoutThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> connection.getNetworkTimeout());
  }

  @Test
  void testSetClientInfoStringDelegates() throws Exception {
    connection.setClientInfo("key", "value");
    verify(mockInner).setClientInfo("key", "value");
  }

  @Test
  void testSetClientInfoPropertiesDelegates() throws Exception {
    Properties props = new Properties();
    props.setProperty("key", "value");
    connection.setClientInfo(props);
    verify(mockInner).setClientInfo(props);
  }

  @Test
  void testGetClientInfoStringDelegates() throws Exception {
    when(mockInner.getClientInfo("key")).thenReturn("value");
    assertEquals("value", connection.getClientInfo("key"));
  }

  @Test
  void testGetClientInfoPropertiesDelegates() throws Exception {
    Properties props = new Properties();
    when(mockInner.getClientInfo()).thenReturn(props);
    assertEquals(props, connection.getClientInfo());
  }
}
