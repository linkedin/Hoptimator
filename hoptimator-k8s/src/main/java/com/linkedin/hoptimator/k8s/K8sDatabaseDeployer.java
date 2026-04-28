package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.DatabaseDeployable;
import com.linkedin.hoptimator.k8s.models.V1alpha1Database;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseList;
import com.linkedin.hoptimator.k8s.models.V1alpha1DatabaseSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;


/** Deploys a Database object. */
class K8sDatabaseDeployer extends K8sDeployer<V1alpha1Database, V1alpha1DatabaseList> {

  private final DatabaseDeployable database;

  K8sDatabaseDeployer(DatabaseDeployable database, K8sContext context) {
    super(context, K8sApiEndpoints.DATABASES);
    this.database = database;
  }

  @Override
  protected V1alpha1Database toK8sObject() throws SQLException {
    String name = K8sUtils.canonicalizeName(database.name());
    // SQL parser uppercases unquoted identifiers, so use case-insensitive lookup
    Map<String, String> options = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    options.putAll(database.options());
    String url = options.get("url");
    if (url == null) {
      throw new SQLException("Database " + database.name() + " requires a 'url' option.");
    }
    V1alpha1DatabaseSpec spec = new V1alpha1DatabaseSpec()
        .url(url);
    if (options.containsKey("driver")) {
      spec.driver(options.get("driver"));
    }
    if (options.containsKey("schema")) {
      spec.schema(options.get("schema"));
    }
    if (options.containsKey("catalog")) {
      spec.catalog(options.get("catalog"));
    }
    if (options.containsKey("dialect")) {
      spec.dialect(V1alpha1DatabaseSpec.DialectEnum.fromValue(options.get("dialect")));
    }
    return new V1alpha1Database()
        .kind(K8sApiEndpoints.DATABASES.kind())
        .apiVersion(K8sApiEndpoints.DATABASES.apiVersion())
        .metadata(new V1ObjectMeta().name(name))
        .spec(spec);
  }
}
