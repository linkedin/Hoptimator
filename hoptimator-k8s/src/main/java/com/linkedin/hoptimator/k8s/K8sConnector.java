package com.linkedin.hoptimator.k8s;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.util.Template;


/** Configures an abstract Source/Sink by applying TableTemplates */
class K8sConnector implements Connector {

  private final Source source;
  private final K8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> tableTemplateApi;

  K8sConnector(Source source, K8sContext context) {
    this.source = source;
    this.tableTemplateApi = new K8sApi<>(context, K8sApiEndpoints.TABLE_TEMPLATES);
  }

  @Override
  public Map<String, String> configure() throws SQLException {
    Template.Environment env =
        new Template.SimpleEnvironment()
            .with("name", source.database() + "-" + source.table().toLowerCase(Locale.ROOT))
            .with("database", source.database())
            .with("table", source.table())
            .with("pipelineName", source.pipelineName())
            .with(source.options());
    String configs = tableTemplateApi.list()
        .stream()
        .map(x -> x.getSpec())
        .filter(x -> x.getDatabases() == null || x.getDatabases().contains(source.database()))
        .filter(x -> x.getMethods() == null || x.getMethods().contains(K8sUtils.method(source)))
        .filter(x -> x.getConnector() != null)
        .map(x -> x.getConnector())
        .map(x -> new Template.SimpleTemplate(x).render(env))
        .collect(Collectors.joining("\n"));
    Properties props = new Properties();
    try {
      props.load(new StringReader(configs));
    } catch (IOException e) {
      throw new SQLException(e);
    }
    Map<String, String> map = new LinkedHashMap<>();
    props.stringPropertyNames().stream().sorted().forEach(k ->
        map.put(k, props.getProperty(k)));
    return map;
  }
}
