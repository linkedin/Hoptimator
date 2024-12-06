package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class TestSqlScripts extends QuidemTestBase {

  @Test
  @Tag("integration")
  public void k8sDdlScript() throws Exception {
    run("k8s-ddl.id");
  }
}
