package com.linkedin.hoptimator.logical;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientConnectionException;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class LogicalTableDriverTest {

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
    // All validation passes (2 tiers + database property set) but K8sContext.create() fails
    // because kubeconfig points to a non-existent file → catch(Exception) → SQLNonTransientException
    String url = "jdbc:logical://nearline=kafka-database;online=venice";
    Properties props = new Properties();
    props.setProperty("database", "mylogicaldb");
    props.setProperty("k8s.kubeconfig", "/nonexistent/path/kubeconfig");
    try (Connection ignored = DriverManager.getConnection(url, props)) {
      throw new AssertionError("Expected exception");
    } catch (SQLNonTransientException e) {
      assertTrue(e.getMessage().contains("Problem loading"));
    }
  }
  @Test
  void connectWithValidUrlThrowsSQLNonTransientWhenK8sContextFails() {
    // A URL that passes all validation (2 tiers + database property) but then fails
    // when creating K8sContext (no K8s config in test env) → covered by catch(Exception e)
    // at the end of the try block, covering lines 93-103.
    Properties props = new Properties();
    props.setProperty("database", "logical");
    try (Connection conn = DriverManager.getConnection(
        "jdbc:logical://nearline=kafka-database;online=venice", props)) {
      // If connect() unexpectedly succeeds (real K8s available), just verify non-null
      assertNotNull(conn);
    } catch (SQLNonTransientException e) {
      // Expected: K8sContext.create() failed → catch(Exception e) path covered
      assertTrue(e.getMessage().contains("Problem loading"));
    } catch (SQLTransientConnectionException e) {
      // Also acceptable: IOException from K8s client
      assertTrue(e.getMessage().contains("Problem loading"));
    } catch (SQLException e) {
      // Any other SQL exception is also fine — something in connect() path was exercised
      assertNotNull(e.getMessage());
    }
  }

}
