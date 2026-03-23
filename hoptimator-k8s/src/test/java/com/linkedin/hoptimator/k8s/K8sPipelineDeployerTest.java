package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


class K8sPipelineDeployerTest {

  @Test
  void toK8sObjectSetsPipelineFields() throws SQLException {
    K8sPipelineDeployer deployer = new K8sPipelineDeployer(
        "my-pipeline", Arrays.asList("spec1", "spec2"), "SELECT 1", null);

    V1alpha1Pipeline pipeline = deployer.toK8sObject();

    assertEquals("my-pipeline", pipeline.getMetadata().getName());
    assertEquals("SELECT 1", pipeline.getSpec().getSql());
    assertEquals("spec1\n---\nspec2", pipeline.getSpec().getYaml());
    assertEquals("Pipeline", pipeline.getKind());
    assertNotNull(pipeline.getApiVersion());
  }

  @Test
  void toK8sObjectWithSingleSpec() throws SQLException {
    K8sPipelineDeployer deployer = new K8sPipelineDeployer(
        "single", Arrays.asList("only-spec"), "SELECT 1", null);

    V1alpha1Pipeline pipeline = deployer.toK8sObject();

    assertEquals("only-spec", pipeline.getSpec().getYaml());
  }
}
