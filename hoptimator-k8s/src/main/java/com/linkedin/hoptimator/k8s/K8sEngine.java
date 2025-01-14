package com.linkedin.hoptimator.k8s;

import javax.sql.DataSource;

import com.linkedin.hoptimator.Engine;
import com.linkedin.hoptimator.SqlDialect;

import org.apache.calcite.adapter.jdbc.JdbcSchema;


public class K8sEngine implements Engine {

  private final String name;
  private final String url;
  private final SqlDialect dialect;
  private final String driver;

  public K8sEngine(String name, String url, SqlDialect dialect, String driver) {
    this.name = name;
    this.url = url;
    this.dialect = dialect;
    this.driver = driver;
  }

  @Override
  public String engineName() {
    return name;
  }

  @Override
  public DataSource dataSource() {
    // TODO support username, password via Secrets
    return JdbcSchema.dataSource(url, driver, null, null);
  }

  @Override
  public SqlDialect dialect() {
    return SqlDialect.FLINK;  // TODO fix hardcoded dialect
  }
}
