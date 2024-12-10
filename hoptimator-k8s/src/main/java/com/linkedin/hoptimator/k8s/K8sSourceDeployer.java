package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.util.Template;


/** Specifies an abstract Source with concrete YAML by applying TableTemplates. */
class K8sSourceDeployer extends K8sYamlDeployer<Source> {

  private final K8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> tableTemplateApi;

  K8sSourceDeployer(K8sContext context) {
    super(context);
    this.tableTemplateApi = new K8sApi<>(context, K8sApiEndpoints.TABLE_TEMPLATES);
  }

  @Override
  public List<String> specify(Source source) throws SQLException {
    Template.Environment env =
        Template.Environment.EMPTY.with("name", source.database() + "-" + source.table().toLowerCase(Locale.ROOT))
            .with("database", source.database())
            .with("schema", source.schema())
            .with("table", source.table())
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
