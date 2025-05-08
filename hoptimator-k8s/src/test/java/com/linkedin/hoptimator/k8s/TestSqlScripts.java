package com.linkedin.hoptimator.k8s;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;


public class TestSqlScripts extends QuidemTestBase {

  @Test
  @Tag("integration")
  public void k8sDdlScript() throws Exception {
    run("k8s-ddl.id");
  }

  @Test
  @Tag("integration")
  public void k8sDdlScriptFunction() throws Exception {
    run("k8s-ddl-function.id", "fun=mysql");
  }

  @Test
  @Tag("integration")
  public void k8sValidationScript() throws Exception {
    run("k8s-validation.id");
  }
}
