package com.linkedin.hoptimator.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
class DelegatingStatementTest {

  @Mock
  private Connection mockConnection;

  @Mock
  private Statement mockStatement;

  @Mock
  private ResultSet mockResultSet;

  private DelegatingStatement delegatingStatement;

  @BeforeEach
  void setUp() {
    delegatingStatement = new DelegatingStatement(mockConnection);
  }

  @Test
  void testExecuteQuerySingleStatement() throws Exception {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery("SELECT 1")).thenReturn(mockResultSet);

    try (ResultSet result = delegatingStatement.executeQuery("SELECT 1")) {
      assertSame(mockResultSet, result);
    }
  }

  @Test
  void testExecuteReturnsTrueAndSetsResultSet() throws Exception {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery("SELECT 1")).thenReturn(mockResultSet);

    boolean result = delegatingStatement.execute("SELECT 1");

    assertTrue(result);
    assertSame(mockResultSet, delegatingStatement.getResultSet());
  }

  @Test
  void testExecuteQueryMultiStatement() throws Exception {
    Statement mockDdlStatement = mock(Statement.class);
    when(mockConnection.createStatement()).thenReturn(mockDdlStatement).thenReturn(mockStatement);
    when(mockStatement.executeQuery("SELECT 1")).thenReturn(mockResultSet);

    try (ResultSet result = delegatingStatement.executeQuery("CREATE TABLE t(id INT);\nSELECT 1")) {
      verify(mockDdlStatement).execute("CREATE TABLE t(id INT)");
      verify(mockDdlStatement).close();
      assertSame(mockResultSet, result);
    }
  }

  @Test
  void testGetConnectionReturnsInjectedConnection() throws Exception {
    assertEquals(mockConnection, delegatingStatement.getConnection());
  }

  @Test
  void testIsClosedDelegatesToConnection() throws Exception {
    when(mockConnection.isClosed()).thenReturn(true);

    assertTrue(delegatingStatement.isClosed());
  }

  // isClosed() must return false on an open statement
  @Test
  void testIsClosedReturnsFalseOnOpenStatement() throws Exception {
    when(mockConnection.isClosed()).thenReturn(false);

    assertFalse(delegatingStatement.isClosed());
  }

  @Test
  void testClearWarningsIsNop() throws Exception {
    delegatingStatement.clearWarnings();
  }

  @Test
  void testGetWarningsReturnsNull() throws Exception {
    assertNull(delegatingStatement.getWarnings());
  }

  @Test
  void testCancelDelegatesToStatement() throws Exception {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    try (ResultSet rs = delegatingStatement.executeQuery("SELECT 1")) {
      assertNotNull(rs);
    }

    delegatingStatement.cancel();

    verify(mockStatement).cancel();
  }

  @Test
  void testCloseDelegatesToStatementAndResultSet() throws Exception {
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    delegatingStatement.executeQuery("SELECT 1"); // ResultSet closed by delegatingStatement.close() below

    delegatingStatement.close();

    verify(mockStatement).close();
    verify(mockResultSet).close();
  }

  @Test
  void testExecuteUpdateThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.executeUpdate("INSERT INTO t VALUES(1)"));
  }

  @Test
  void testGetUpdateCountThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getUpdateCount());
  }

  @Test
  void testGetMaxFieldSizeThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getMaxFieldSize());
  }

  @Test
  void testSetMaxFieldSizeThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.setMaxFieldSize(100));
  }

  @Test
  void testGetMaxRowsThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getMaxRows());
  }

  @Test
  void testSetMaxRowsThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.setMaxRows(100));
  }

  @Test
  void testUnwrapReturnsNull() throws Exception {
    assertNull(delegatingStatement.unwrap(String.class));
  }

  @Test
  void testIsWrapperForReturnsFalse() throws Exception {
    assertFalse(delegatingStatement.isWrapperFor(String.class));
  }

  @Test
  void testAddBatchThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.addBatch("SELECT 1"));
  }

  @Test
  void testExecuteBatchThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.executeBatch());
  }

  @Test
  void testGetMoreResultsThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getMoreResults());
  }

  @Test
  void testCloseOnCompletionThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.closeOnCompletion());
  }

  @Test
  void testIsCloseOnCompletionThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.isCloseOnCompletion());
  }

  @Test
  void testSetEscapeProcessingThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.setEscapeProcessing(true));
  }

  @Test
  void testGetQueryTimeoutThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getQueryTimeout());
  }

  @Test
  void testSetQueryTimeoutThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.setQueryTimeout(30));
  }

  @Test
  void testSetCursorNameThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.setCursorName("cursor1"));
  }

  @Test
  void testSetFetchDirectionThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.setFetchDirection(1));
  }

  @Test
  void testGetFetchDirectionThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getFetchDirection());
  }

  @Test
  void testSetFetchSizeThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.setFetchSize(100));
  }

  @Test
  void testGetFetchSizeThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getFetchSize());
  }

  @Test
  void testGetResultSetConcurrencyThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getResultSetConcurrency());
  }

  @Test
  void testGetResultSetTypeThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getResultSetType());
  }

  @Test
  void testClearBatchThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.clearBatch());
  }

  @Test
  void testGetMoreResultsWithCurrentThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getMoreResults(1));
  }

  @Test
  void testGetGeneratedKeysThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getGeneratedKeys());
  }

  @Test
  void testExecuteUpdateWithAutoGeneratedKeysThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> delegatingStatement.executeUpdate("INSERT INTO t VALUES(1)", 1));
  }

  @Test
  void testExecuteUpdateWithColumnIndexesThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> delegatingStatement.executeUpdate("INSERT INTO t VALUES(1)", new int[]{1}));
  }

  @Test
  void testExecuteUpdateWithColumnNamesThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> delegatingStatement.executeUpdate("INSERT INTO t VALUES(1)", new String[]{"col1"}));
  }

  @Test
  void testExecuteWithAutoGeneratedKeysThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> delegatingStatement.execute("SELECT 1", 1));
  }

  @Test
  void testExecuteWithColumnIndexesThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> delegatingStatement.execute("SELECT 1", new int[]{1}));
  }

  @Test
  void testExecuteWithColumnNamesThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class,
        () -> delegatingStatement.execute("SELECT 1", new String[]{"col1"}));
  }

  @Test
  void testGetResultSetHoldabilityThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.getResultSetHoldability());
  }

  @Test
  void testSetPoolableThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.setPoolable(true));
  }

  @Test
  void testIsPoolableThrowsUnsupported() {
    assertThrows(SQLFeatureNotSupportedException.class, () -> delegatingStatement.isPoolable());
  }

  @Test
  void testCloseWhenNotUsedDoesNotThrow() throws Exception {
    delegatingStatement.close();
    // Should not throw - statement and resultSet are null
  }
}
