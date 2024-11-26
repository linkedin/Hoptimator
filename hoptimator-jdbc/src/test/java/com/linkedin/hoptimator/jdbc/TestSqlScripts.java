package com.linkedin.hoptimator.jdbc;

import org.junit.jupiter.api.Test;

public class TestSqlScripts extends QuidemTestBase {

  @Test
  public void basicDdlScript() throws Exception {
    run("basic-ddl.id");
  }
}
