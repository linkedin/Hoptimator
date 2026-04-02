package com.linkedin.hoptimator.jdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;


@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE"},
    justification = "Mock objects created in stubbing setup don't need resource management")
@ExtendWith(MockitoExtension.class)
class HoptimatorDdlExecutorTest {

  @Mock
  private CalciteConnection mockCalciteConnection;

  @Test
  void testConstructorThrowsForReadOnlyConnection() throws SQLException {
    when(mockCalciteConnection.isReadOnly()).thenReturn(true);
    Properties properties = new Properties();
    HoptimatorConnection connection = new HoptimatorConnection(mockCalciteConnection, properties);

    HoptimatorDdlExecutor.DdlException exception = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> new HoptimatorDdlExecutor(connection));

    assertTrue(exception.getMessage().contains("read-only"));
  }

  @Test
  void testConstructorSucceedsForWritableConnection() throws SQLException {
    when(mockCalciteConnection.isReadOnly()).thenReturn(false);
    Properties properties = new Properties();
    HoptimatorConnection connection = new HoptimatorConnection(mockCalciteConnection, properties);

    HoptimatorDdlExecutor executor = new HoptimatorDdlExecutor(connection);

    assertNotNull(executor);
  }

  @Test
  void testParserFactoryIsNotNull() {
    SqlParserImplFactory factory = HoptimatorDdlExecutor.PARSER_FACTORY;

    assertNotNull(factory);
  }

  @Test
  void testParserFactoryGetDdlExecutorReturnsInstance() {
    DdlExecutor executor = HoptimatorDdlExecutor.PARSER_FACTORY.getDdlExecutor();

    assertNotNull(executor);
  }

  @Test
  void testDdlExceptionWithMessage() {
    HoptimatorDdlExecutor.DdlException exception = new HoptimatorDdlExecutor.DdlException("test error");

    assertEquals("test error", exception.getMessage());
  }

  @Test
  void testDdlExceptionWithMessageAndCause() {
    RuntimeException cause = new RuntimeException("root cause");
    HoptimatorDdlExecutor.DdlException exception = new HoptimatorDdlExecutor.DdlException("test error", cause);

    assertEquals("test error", exception.getMessage());
    assertEquals(cause, exception.getCause());
  }

  @Test
  void testDdlExceptionWithSqlNode() {
    SqlIdentifier node = new SqlIdentifier("myView", SqlParserPos.ZERO);

    HoptimatorDdlExecutor.DdlException exception = new HoptimatorDdlExecutor.DdlException(node, "something went wrong");

    assertNotNull(exception.getMessage());
    assertTrue(exception.getMessage().contains("something went wrong"));
    assertTrue(exception.getMessage().contains("myView"));
  }

  @Test
  void testDdlExceptionWithSqlNodeAndCause() {
    SqlIdentifier node = new SqlIdentifier("myView", new SqlParserPos(5, 10));
    RuntimeException cause = new RuntimeException("root");

    HoptimatorDdlExecutor.DdlException exception = new HoptimatorDdlExecutor.DdlException(node, "failed", cause);

    assertTrue(exception.getMessage().contains("failed"));
    assertTrue(exception.getMessage().contains("line 5"));
    assertTrue(exception.getMessage().contains("col 10"));
    assertEquals(cause, exception.getCause());
  }

  @Test
  void testDdlExceptionWithNullCause() {
    HoptimatorDdlExecutor.DdlException exception = new HoptimatorDdlExecutor.DdlException("msg", null);

    assertEquals("msg", exception.getMessage());
  }

  @Test
  void testParserFactoryCreatesParser() {
    SqlAbstractParserImpl parser = HoptimatorDdlExecutor.PARSER_FACTORY.getParser(new StringReader("SELECT 1"));

    assertNotNull(parser);
  }

  @Test
  void testConstructorThrowsWhenIsReadOnlyThrowsSQLException() throws SQLException {
    when(mockCalciteConnection.isReadOnly()).thenThrow(new SQLException("connection error"));
    Properties properties = new Properties();
    HoptimatorConnection conn = new HoptimatorConnection(mockCalciteConnection, properties);

    HoptimatorDdlExecutor.DdlException exception = assertThrows(
        HoptimatorDdlExecutor.DdlException.class,
        () -> new HoptimatorDdlExecutor(conn));

    assertTrue(exception.getMessage().contains("read-only"));
  }
}
