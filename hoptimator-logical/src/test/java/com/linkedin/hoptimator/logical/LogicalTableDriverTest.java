package com.linkedin.hoptimator.logical;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.Properties;

import com.linkedin.hoptimator.k8s.K8sContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;


@ExtendWith(MockitoExtension.class)
public class LogicalTableDriverTest {

  // K8sContext.create() reads the developer's real ~/.kube/config when no k8s connection
  // properties are supplied, which blocks on an interactive cluster login. The failure-path
  // tests below stub it via this static mock so they stay hermetic. Validation tests that
  // return before reaching K8sContext.create() simply leave it unstubbed.
  @Mock
  private MockedStatic<K8sContext> k8sContextStatic;

  @Test
  public void connectReturnsNullForNonLogicalUrl() throws Exception {
    LogicalTableDriver driver = new LogicalTableDriver();
    Connection conn = driver.connect("jdbc:other://something", new Properties());
    assertNull(conn);
  }

  @Test
  public void connectThrowsWhenFewerThanTwoTiers() throws Exception {
    String url = "jdbc:logical://nearline=kafka-database";
    try (Connection ignored = DriverManager.getConnection(url, new Properties())) {
      throw new AssertionError("Expected SQLNonTransientException");
    } catch (SQLNonTransientException e) {
      assertTrue(e.getMessage().contains("at least 2 tiers"));
    }
  }

  @Test
  public void connectThrowsWhenNoTiersInUrl() throws Exception {
    String url = "jdbc:logical://";
    try (Connection ignored = DriverManager.getConnection(url, new Properties())) {
      throw new AssertionError("Expected SQLNonTransientException");
    } catch (SQLNonTransientException e) {
      assertTrue(e.getMessage().contains("at least 2 tiers"));
    }
  }

  @Test
  public void connectThrowsWhenDatabasePropertyMissing() throws Exception {
    // Two valid tiers but no "database" property — should throw before calling super.connect()
    String url = "jdbc:logical://nearline=kafka-database;online=venice";
    try (Connection ignored = DriverManager.getConnection(url, new Properties())) {
      throw new AssertionError("Expected SQLNonTransientException");
    } catch (SQLNonTransientException e) {
      assertTrue(e.getMessage().contains("Missing 'database' property"));
    }
  }

  @Test
  public void connectThrowsWithMixedCaseTiers() throws Exception {
    // Only one recognizable tier — should throw for < 2 tiers
    String url = "jdbc:logical://nearline=kafka-database;unknownkey=foo";
    try (Connection ignored = DriverManager.getConnection(url, new Properties())) {
      throw new AssertionError("Expected SQLNonTransientException");
    } catch (SQLNonTransientException e) {
      assertTrue(e.getMessage().contains("at least 2 tiers"));
    }
  }

  @Test
  public void connectThrowsWhenDatabasePropertyIsEmpty() throws Exception {
    // Two valid tiers and database="" (empty string, not null) — should throw missing database
    String url = "jdbc:logical://nearline=kafka-database;online=venice";
    Properties props = new Properties();
    props.setProperty("database", "");
    try (Connection ignored = DriverManager.getConnection(url, props)) {
      throw new AssertionError("Expected SQLNonTransientException");
    } catch (SQLNonTransientException e) {
      assertTrue(e.getMessage().contains("Missing 'database' property"));
    }
  }

  @Test
  public void connectThrowsWhenDatabasePropertyInUrl() throws Exception {
    // database= embedded in URL (not in props) — should also throw missing database
    // because database= empty string
    String url = "jdbc:logical://nearline=kafka-database;online=venice;database=";
    try (Connection ignored = DriverManager.getConnection(url, new Properties())) {
      throw new AssertionError("Expected SQLNonTransientException");
    } catch (SQLNonTransientException e) {
      assertTrue(e.getMessage().contains("Missing 'database' property"));
    }
  }

  @Test
  public void connectThrowsNonTransientWhenK8sContextCreationFails() throws Exception {
    // All validation passes (2 tiers + database property set) but K8sContext.create() fails.
    // Stub it to throw rather than letting it read the real ~/.kube/config and block on login.
    k8sContextStatic.when(() -> K8sContext.create(any()))
        .thenThrow(new RuntimeException("simulated K8sContext failure"));

    String url = "jdbc:logical://nearline=kafka-database;online=venice";
    Properties props = new Properties();
    props.setProperty("database", "mylogicaldb");

    SQLException ex = assertThrows(SQLNonTransientException.class,
        () -> DriverManager.getConnection(url, props));
    assertTrue(ex.getMessage().contains("Problem loading"));
  }

  @Test
  void connectPreservesCauseWhenK8sContextCreationFails() {
    // The catch(Exception e) branch must wrap the underlying failure as the cause rather than
    // swallowing it. Stub K8sContext.create() to throw a known exception and assert it is preserved.
    RuntimeException boom = new RuntimeException("boom");
    k8sContextStatic.when(() -> K8sContext.create(any())).thenThrow(boom);

    Properties props = new Properties();
    props.setProperty("database", "logical");

    SQLException ex = assertThrows(SQLNonTransientException.class,
        () -> DriverManager.getConnection(
            "jdbc:logical://nearline=kafka-database;online=venice", props));
    assertTrue(ex.getMessage().contains("Problem loading"));
    assertSame(boom, ex.getCause(), "original failure should be preserved as the cause");
  }

}
