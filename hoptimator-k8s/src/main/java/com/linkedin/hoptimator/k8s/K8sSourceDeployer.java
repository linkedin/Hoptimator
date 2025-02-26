package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.util.Template;


/** Specifies an abstract Source with concrete YAML by applying TableTemplates. */
class K8sSourceDeployer extends K8sYamlDeployer {

  private final Source source;
  private final K8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> tableTemplateApi;

  K8sSourceDeployer(Source source, K8sContext context) {
    super(context);
    this.source = source;
    this.tableTemplateApi = new K8sApi<>(context, K8sApiEndpoints.TABLE_TEMPLATES);
  }

  @Override
  public List<String> specify() throws SQLException {
    String name = K8sUtils.canonicalizeName(source.database(), source.table());
    Template.Environment env =
        new Template.SimpleEnvironment()
            .with("name", name)
            .with("database", source.database())
            .with("schema", source.schema())
            .with("table", source.table())
            .with("pipelineName", source.pipelineName())
            .with(source.options());
    return tableTemplateApi.list()
        .stream()
        .map(x -> x.getSpec())
        .filter(x -> x.getDatabases() == null || x.getDatabases().contains(source.database()))
        .filter(x -> x.getMethods() == null || x.getMethods().contains(K8sUtils.method(source)))
        .filter(x -> x.getYaml() != null)
        .map(x -> x.getYaml())
        .map(x -> new Template.SimpleTemplate(x).render(env))
        .collect(Collectors.toList());
  }
}
