package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.View;
import com.linkedin.hoptimator.k8s.models.V1alpha1View;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class K8sViewDeployerTest {

  @Mock
  private View view;

  @Test
  void toK8sObjectSetsViewFields() throws SQLException {
    when(view.path()).thenReturn(Arrays.asList("catalog", "schema", "my-view"));
    when(view.viewSql()).thenReturn("SELECT * FROM t");

    K8sViewDeployer deployer = new K8sViewDeployer(view, false, null);
    V1alpha1View v1View = deployer.toK8sObject();

    assertEquals("catalog-schema-my-view", v1View.getMetadata().getName());
    assertEquals("my-view", v1View.getSpec().getView());
    assertEquals("schema", v1View.getSpec().getSchema());
    assertEquals("catalog", v1View.getSpec().getCatalog());
    assertEquals("SELECT * FROM t", v1View.getSpec().getSql());
    assertFalse(v1View.getSpec().getMaterialized());
    assertEquals("View", v1View.getKind());
    assertNotNull(v1View.getApiVersion());
  }

  @Test
  void toK8sObjectWithMaterializedTrue() throws SQLException {
    when(view.path()).thenReturn(Arrays.asList("cat", "sch", "vw"));
    when(view.viewSql()).thenReturn("SELECT 1");

    K8sViewDeployer deployer = new K8sViewDeployer(view, true, null);
    V1alpha1View v1View = deployer.toK8sObject();

    assertTrue(v1View.getSpec().getMaterialized());
  }

  @Test
  void toK8sObjectWithTwoPartPath() throws SQLException {
    when(view.path()).thenReturn(Arrays.asList("schema", "my-view"));
    when(view.viewSql()).thenReturn("SELECT 1");

    K8sViewDeployer deployer = new K8sViewDeployer(view, false, null);
    V1alpha1View v1View = deployer.toK8sObject();

    assertEquals("my-view", v1View.getSpec().getView());
    assertEquals("schema", v1View.getSpec().getSchema());
  }
}
