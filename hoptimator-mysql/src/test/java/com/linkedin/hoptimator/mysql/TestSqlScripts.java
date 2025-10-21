package com.linkedin.hoptimator.mysql;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;


@Tag("integration")
public class TestSqlScripts extends QuidemTestBase {

  @Test
  public void mysqlDdlScript() throws Exception {
    run("mysql-ddl.id");
  }
}
