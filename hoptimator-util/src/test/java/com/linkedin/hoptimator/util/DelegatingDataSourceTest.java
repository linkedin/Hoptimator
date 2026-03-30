package com.linkedin.hoptimator.util;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "ODR_OPEN_DATABASE_RESOURCE", "DMI_CONSTANT_DB_PASSWORD"},
    justification = "Test uses invalid credentials intentionally; getConnection() throws before returning a resource")
class DelegatingDataSourceTest {

  private DelegatingDataSource dataSource;

  @BeforeEach
  void setUp() {
    dataSource = new DelegatingDataSource();
  }

  @Test
  void testDefaultLoginTimeout() {
    assertEquals(60, dataSource.getLoginTimeout());
  }

  @Test
  void testSetLoginTimeout() {
    dataSource.setLoginTimeout(120);

    assertEquals(120, dataSource.getLoginTimeout());
  }

  @Test
  void testGetLogWriterReturnsNonNull() {
    assertNotNull(dataSource.getLogWriter());
  }

  @Test
  void testSetLogWriter() {
    PrintWriter writer = new PrintWriter(new OutputStreamWriter(System.err, StandardCharsets.UTF_8));

    dataSource.setLogWriter(writer);

    assertEquals(writer, dataSource.getLogWriter());
  }

  @Test
  void testGetParentLoggerReturnsNonNull() {
    assertNotNull(dataSource.getParentLogger());
  }

  @Test
  void testIsWrapperForReturnsFalse() throws Exception {
    assertFalse(dataSource.isWrapperFor(String.class));
  }

  @Test
  void testUnwrapReturnsNull() throws Exception {
    assertNull(dataSource.unwrap(String.class));
  }

  @Test
  void testSetUrlAndGetConnectionThrowsForBadUrl() {
    dataSource.setUrl("jdbc:nonexistent://localhost/db");
    SQLException ex = new SQLException("No suitable driver");
    mockedDriverManager.when(() -> DriverManager.getConnection("jdbc:nonexistent://localhost/db"))
        .thenThrow(ex);

    assertThrows(SQLException.class, () -> dataSource.getConnection());
  }

  @Test
  void testGetConnectionWithCredentialsThrowsForBadUrl() {
    dataSource.setUrl("jdbc:nonexistent://localhost/db");
    SQLException ex = new SQLException("No suitable driver");
    mockedDriverManager.when(() -> DriverManager.getConnection("jdbc:nonexistent://localhost/db", "user", "pass"))
        .thenThrow(ex);

    assertThrows(SQLException.class, () -> dataSource.getConnection("user", "pass"));
  }

  @Mock
  private Connection mockConnection;

  @Mock
  private MockedStatic<DriverManager> mockedDriverManager;

  @Test
  void testGetConnectionReturnsWrappedConnection() throws SQLException {
    dataSource.setUrl("jdbc:test://localhost/db");

    mockedDriverManager.when(() -> DriverManager.getConnection("jdbc:test://localhost/db"))
        .thenReturn(mockConnection);

    Connection conn = dataSource.getConnection();

    assertNotNull(conn);
    assertTrue(conn instanceof DelegatingConnection);
  }

  @Test
  void testGetConnectionWithCredentialsReturnsWrappedConnection() throws SQLException {
    dataSource.setUrl("jdbc:test://localhost/db");

    mockedDriverManager.when(() -> DriverManager.getConnection("jdbc:test://localhost/db", "user", "pass"))
        .thenReturn(mockConnection);

    Connection conn = dataSource.getConnection("user", "pass");

    assertNotNull(conn);
    assertTrue(conn instanceof DelegatingConnection);
  }
}
