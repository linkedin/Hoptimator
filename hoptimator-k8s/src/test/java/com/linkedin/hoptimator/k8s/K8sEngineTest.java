package com.linkedin.hoptimator.k8s;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.SqlDialect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


class K8sEngineTest {

  @Test
  void engineNameReturnsName() {
    K8sEngine engine = new K8sEngine("flink", "jdbc:flink://localhost", SqlDialect.FLINK, "org.flink.Driver");
    assertEquals("flink", engine.engineName());
  }

  @Test
  void urlReturnsUrl() {
    K8sEngine engine = new K8sEngine("flink", "jdbc:flink://localhost", SqlDialect.FLINK, "org.flink.Driver");
    assertEquals("jdbc:flink://localhost", engine.url());
  }

  @Test
  void dialectReturnsDialect() {
    K8sEngine engine = new K8sEngine("flink", "jdbc:flink://localhost", SqlDialect.FLINK, "org.flink.Driver");
    assertEquals(SqlDialect.FLINK, engine.dialect());
  }

  @Test
  void dataSourceReturnsNonNull() {
    K8sEngine engine = new K8sEngine("flink", "jdbc:flink://localhost", SqlDialect.FLINK, "org.flink.Driver");
    DataSource ds = engine.dataSource();
    assertNotNull(ds);
  }

  @Test
  void nullUrlThrowsNullPointerException() {
    assertThrows(NullPointerException.class,
        () -> new K8sEngine("flink", null, SqlDialect.FLINK, "org.flink.Driver"));
  }
}
