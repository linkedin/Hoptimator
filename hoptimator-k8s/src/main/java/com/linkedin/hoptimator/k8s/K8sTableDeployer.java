package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.stream.Collectors;

import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.TableSource;
import com.linkedin.hoptimator.k8s.models.V1alpha1Table;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableSpecColumn;


/** Deploys a Table CRD object from a TableSource. */
class K8sTableDeployer extends K8sDeployer<V1alpha1Table, V1alpha1TableList> {

  private final TableSource tableSource;

  K8sTableDeployer(TableSource tableSource, K8sContext context) {
    super(context, K8sApiEndpoints.TABLES);
    this.tableSource = tableSource;
  }

  @Override
  protected V1alpha1Table toK8sObject() throws SQLException {
    String name = K8sUtils.canonicalizeName(tableSource.database(), tableSource.table());
    LinkedList<String> q = new LinkedList<>(tableSource.path());
    return new V1alpha1Table()
        .kind(K8sApiEndpoints.TABLES.kind())
        .apiVersion(K8sApiEndpoints.TABLES.apiVersion())
        .metadata(new V1ObjectMeta()
            .name(name))
        .spec(new V1alpha1TableSpec()
            .table(q.pollLast())
            .schema(q.pollLast())
            .catalog(q.pollLast())
            .database(tableSource.database())
            .columns(tableSource.columns().stream()
                .map(col -> new V1alpha1TableSpecColumn()
                    .name(col.name())
                    .type(col.typeName())
                    .nullable(col.nullable()))
                .collect(Collectors.toList()))
            .options(tableSource.options()));
  }
}
