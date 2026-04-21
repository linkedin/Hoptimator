package com.linkedin.hoptimator.logical;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


/**
 * Unit tests for {@link K8sLogicalTableDeployer#toK8sObject()}.
 *
 * <p>These tests verify the structure of the {@code LogicalTable} CRD object built by the
 * deployer — metadata (name, DATABASE_LABEL), spec (tableName, tiers map). They complement
 * {@link LogicalTableDeployerTest} which now mocks the CRD deployer and delegates content
 * checks here.
 */
class K8sLogicalTableDeployerTest {

  @Test
  void toK8sObjectHasCorrectName() {
    Map<String, String> tierMap = Map.of("nearline", "kafka-db", "offline", "openhouse-db");
    K8sLogicalTableDeployer deployer = new K8sLogicalTableDeployer(
        "logical-testevent", "logical", "testevent", tierMap, null);

    V1alpha1LogicalTable crd = deployer.toK8sObject();

    assertEquals("logical-testevent", crd.getMetadata().getName());
  }

  @Test
  void toK8sObjectHasDatabaseLabel() {
    Map<String, String> tierMap = Map.of("nearline", "kafka-db");
    K8sLogicalTableDeployer deployer = new K8sLogicalTableDeployer(
        "logical-myevent", "logical", "myevent", tierMap, null);

    V1alpha1LogicalTable crd = deployer.toK8sObject();

    assertNotNull(crd.getMetadata().getLabels());
    assertEquals("logical",
        crd.getMetadata().getLabels().get(LogicalTableDriver.DATABASE_LABEL));
  }

  @Test
  void toK8sObjectHasCorrectTableName() {
    Map<String, String> tierMap = Map.of("nearline", "kafka-db");
    K8sLogicalTableDeployer deployer = new K8sLogicalTableDeployer(
        "logical-testevent", "logical", "testevent", tierMap, null);

    V1alpha1LogicalTable crd = deployer.toK8sObject();

    assertEquals("testevent", crd.getSpec().getTableName());
  }

  @Test
  void toK8sObjectHasCorrectTiersMap() {
    Map<String, String> tierMap = Map.of("nearline", "kafka-db", "offline", "openhouse-db");
    K8sLogicalTableDeployer deployer = new K8sLogicalTableDeployer(
        "logical-testevent", "logical", "testevent", tierMap, null);

    V1alpha1LogicalTable crd = deployer.toK8sObject();

    assertEquals(2, crd.getSpec().getTiers().size());
    assertEquals("kafka-db", crd.getSpec().getTiers().get("nearline").getDatabase());
    assertEquals("openhouse-db", crd.getSpec().getTiers().get("offline").getDatabase());
  }

  @Test
  void toK8sObjectHasCorrectMetadata() {
    Map<String, String> tierMap = Map.of("nearline", "kafka-db", "offline", "openhouse-db");
    K8sLogicalTableDeployer deployer = new K8sLogicalTableDeployer(
        "logical-testevent", "logical", "testevent", tierMap, null);

    V1alpha1LogicalTable crd = deployer.toK8sObject();

    assertEquals("logical-testevent", crd.getMetadata().getName());
    assertEquals("logical",
        crd.getMetadata().getLabels().get(LogicalTableDriver.DATABASE_LABEL));
    assertEquals("testevent", crd.getSpec().getTableName());
    assertEquals(2, crd.getSpec().getTiers().size());
    assertEquals("kafka-db", crd.getSpec().getTiers().get("nearline").getDatabase());
  }

  @Test
  void toK8sObjectWithSingleTier() {
    Map<String, String> tierMap = Map.of("online", "venice-db");
    K8sLogicalTableDeployer deployer = new K8sLogicalTableDeployer(
        "logical-orders", "mydb", "orders", tierMap, null);

    V1alpha1LogicalTable crd = deployer.toK8sObject();

    assertEquals("logical-orders", crd.getMetadata().getName());
    assertEquals("mydb", crd.getMetadata().getLabels().get(LogicalTableDriver.DATABASE_LABEL));
    assertEquals("orders", crd.getSpec().getTableName());
    assertEquals(1, crd.getSpec().getTiers().size());
    assertEquals("venice-db", crd.getSpec().getTiers().get("online").getDatabase());
  }

  @Test
  void toK8sObjectWithThreeTiers() {
    Map<String, String> tierMap = Map.of(
        "nearline", "kafka-db",
        "offline", "openhouse-db",
        "online", "venice-db");
    K8sLogicalTableDeployer deployer = new K8sLogicalTableDeployer(
        "logical-events", "mydb", "events", tierMap, null);

    V1alpha1LogicalTable crd = deployer.toK8sObject();

    assertEquals(3, crd.getSpec().getTiers().size());
    assertEquals("kafka-db", crd.getSpec().getTiers().get("nearline").getDatabase());
    assertEquals("openhouse-db", crd.getSpec().getTiers().get("offline").getDatabase());
    assertEquals("venice-db", crd.getSpec().getTiers().get("online").getDatabase());
  }
}
