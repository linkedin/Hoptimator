package com.linkedin.hoptimator.k8s;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.jdbc.QuidemTestBase;


@Tag("integration")
public class TestSqlScripts extends QuidemTestBase {

  @Test
  public void k8sDdlScript() throws Exception {
    run("k8s-ddl.id");
  }

  @Test
  public void k8sDdlScriptFunction() throws Exception {
    run("k8s-ddl-function.id", "fun=mysql");
  }

  @Test
  public void k8sValidationScript() throws Exception {
    run("k8s-validation.id");
  }

  @Test
  public void k8sMetadataTables() throws Exception {
    run("k8s-metadata.id", "hints=offline.table.name=ads_offline");
  }

  @Test
  public void k8sConditionalJobTemplate() throws Exception {
    run("k8s-metadata-beam.id", "hints=flink.app.type=BEAM");
  }

  @Test
  public void k8sTriggerPauseResume() throws Exception {
    run("k8s-trigger-pause.id");
  }
}
