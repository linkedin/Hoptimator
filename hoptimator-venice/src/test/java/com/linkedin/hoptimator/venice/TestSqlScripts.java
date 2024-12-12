package com.linkedin.hoptimator.venice;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;


public class TestSqlScripts extends QuidemTestBase {

  @Test
  @Tag("integration")
  public void veniceDdlScript() throws Exception {
    run("venice-ddl.id");
  }
}
