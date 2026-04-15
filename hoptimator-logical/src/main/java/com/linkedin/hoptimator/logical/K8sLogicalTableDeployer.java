package com.linkedin.hoptimator.logical;

import java.util.Map;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sDeployer;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTable;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableList;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1LogicalTableSpecTiers;


/**
 * Deploys a {@code LogicalTable} CRD, analogous to {@link com.linkedin.hoptimator.k8s.K8sPipelineDeployer}
 * for Pipeline CRDs. Inherits snapshot-based restore from {@link K8sDeployer}: if the CRD did not
 * exist before this deployer ran, restore() deletes it; if it existed, restore() reverts it to its
 * prior state.
 */
class K8sLogicalTableDeployer extends K8sDeployer<V1alpha1LogicalTable, V1alpha1LogicalTableList> {

  private final String crdName;
  private final String databaseLabel;
  private final String tableName;
  private final Map<String, String> tierMap;

  K8sLogicalTableDeployer(String crdName, String databaseLabel, String tableName,
      Map<String, String> tierMap, K8sContext context) {
    super(context, K8sApiEndpoints.LOGICAL_TABLES);
    this.crdName = crdName;
    this.databaseLabel = databaseLabel;
    this.tableName = tableName;
    this.tierMap = tierMap;
  }

  @Override
  protected V1alpha1LogicalTable toK8sObject() {
    V1alpha1LogicalTableSpec spec = new V1alpha1LogicalTableSpec();
    spec.tableName(tableName);
    for (Map.Entry<String, String> entry : tierMap.entrySet()) {
      spec.putTiersItem(entry.getKey(),
          new V1alpha1LogicalTableSpecTiers().databaseCrdName(entry.getValue()));
    }
    return new V1alpha1LogicalTable()
        .kind(K8sApiEndpoints.LOGICAL_TABLES.kind())
        .apiVersion(K8sApiEndpoints.LOGICAL_TABLES.apiVersion())
        .metadata(new V1ObjectMeta().name(crdName)
            .putLabelsItem(LogicalTableDriver.DATABASE_LABEL, databaseLabel))
        .spec(spec);
  }
}
