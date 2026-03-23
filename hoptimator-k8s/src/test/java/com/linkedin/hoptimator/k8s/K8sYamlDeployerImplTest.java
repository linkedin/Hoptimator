package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


class K8sYamlDeployerImplTest {

  @Test
  void specifyReturnsProvidedSpecs() throws SQLException {
    List<String> specs = Arrays.asList("spec1", "spec2", "spec3");
    K8sYamlDeployerImpl deployer = new K8sYamlDeployerImpl(null, specs);

    List<String> result = deployer.specify();

    assertEquals(3, result.size());
    assertEquals("spec1", result.get(0));
    assertEquals("spec2", result.get(1));
    assertEquals("spec3", result.get(2));
  }

  @Test
  void specifyReturnsEmptyListWhenNoSpecs() throws SQLException {
    K8sYamlDeployerImpl deployer = new K8sYamlDeployerImpl(null, Collections.emptyList());

    List<String> result = deployer.specify();

    assertEquals(0, result.size());
  }
}
