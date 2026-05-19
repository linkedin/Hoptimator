package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.SqlJobDeployable;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJob;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJobList;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJobSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.sql.SQLException;
import java.util.Map;


/** Deploys a SqlJob object. */
class K8sSqlJobDeployer extends K8sDeployer<V1alpha1SqlJob, V1alpha1SqlJobList> {

  private final SqlJobDeployable sqlJob;

  K8sSqlJobDeployer(SqlJobDeployable sqlJob, K8sContext context) {
    super(context, K8sApiEndpoints.SQL_JOBS);
    this.sqlJob = sqlJob;
  }

  @Override
  protected V1alpha1SqlJob toK8sObject() throws SQLException {
    String name = K8sUtils.canonicalizeName(sqlJob.name());
    V1alpha1SqlJobSpec spec = new V1alpha1SqlJobSpec()
        .sql(sqlJob.sql());
    if (sqlJob.dialect() != null) {
      spec.dialect(V1alpha1SqlJobSpec.DialectEnum.fromValue(sqlJob.dialect()));
    }
    if (sqlJob.executionMode() != null) {
      spec.executionMode(V1alpha1SqlJobSpec.ExecutionModeEnum.fromValue(sqlJob.executionMode()));
    }
    Map<String, String> options = sqlJob.options();
    if (!options.isEmpty()) {
      spec.configs(options);
    }
    return new V1alpha1SqlJob()
        .kind(K8sApiEndpoints.SQL_JOBS.kind())
        .apiVersion(K8sApiEndpoints.SQL_JOBS.apiVersion())
        .metadata(new V1ObjectMeta().name(name))
        .spec(spec);
  }
}
