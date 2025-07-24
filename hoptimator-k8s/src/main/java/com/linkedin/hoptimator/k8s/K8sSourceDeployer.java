package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.jdbc.HoptimatorConnection;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateSpec;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.Template;

/** Specifies an abstract Source with concrete YAML by applying TableTemplates. */
class K8sSourceDeployer extends K8sYamlDeployer {
  private final K8sContext context;
  private final Source source;
  private final K8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> tableTemplateApi;

  K8sSourceDeployer(Source source, K8sContext context) {
    super(context);
    this.context = context;
    this.source = source;
    this.tableTemplateApi = new K8sApi<>(context, K8sApiEndpoints.TABLE_TEMPLATES);
  }

  @Override
  public List<String> specify() throws SQLException {
    String name = K8sUtils.canonicalizeName(source.database(), source.table());
    try (HoptimatorConnection connection = context.connection()) {
      Template.Environment env =
          new Template.SimpleEnvironment()
              .with("name", name)
              .with("database", source.database())
              .with("schema", source.schema())
              .with("table", source.table())
              .with(source.options())
              .with(DeploymentService.parseHints(connection.connectionProperties()));

      return tableTemplateApi.list()
          .stream()
          .map(V1alpha1TableTemplate::getSpec)
          .filter(Objects::nonNull)
          .filter(x -> x.getDatabases() == null || x.getDatabases().contains(source.database()))
          .filter(x -> x.getMethods() == null || x.getMethods().contains(K8sUtils.method(source)))
          .map(V1alpha1TableTemplateSpec::getYaml)
          .filter(Objects::nonNull)
          .map(x -> new Template.SimpleTemplate(x).render(env))
          .filter(Objects::nonNull) // Filter out null templates (which might have errored out due to missing hint expressions)
          .collect(Collectors.toList());
    }
  }
}
