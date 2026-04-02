package com.linkedin.hoptimator.mysql;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("integration")
public class TestSqlScripts extends QuidemTestBase {

  @Test
  public void mysqlDdlScript() throws Exception {
    run("mysql-ddl.id");
  }

  @Test
  public void mysqlDdlCreateTableScript() throws Exception {
    run("mysql-ddl-create-table.id");
  }
}
