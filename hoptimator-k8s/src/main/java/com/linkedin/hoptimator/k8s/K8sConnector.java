package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.Connector;
import com.linkedin.hoptimator.util.Source;
import com.linkedin.hoptimator.util.Template;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1TableTemplateSpec;

import java.io.StringReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;

/** Configures an abstract Source/Sink by applying TableTemplates */
class K8sConnector implements Connector<Source> {

  private final K8sApi<V1alpha1TableTemplate, V1alpha1TableTemplateList> tableTemplateApi;

  K8sConnector(K8sContext context) {
    this.tableTemplateApi = new K8sApi<>(context, K8sApiEndpoints.TABLE_TEMPLATES);
  }

  @Override
  public Map<String, String> configure(Source source) throws SQLException {
    Template.Environment env = Template.Environment.EMPTY
        .with("name", source.database() + "-" + source.table().toLowerCase(Locale.ROOT))
        .with("database", source.database())
        .with("table", source.table())
        .with(source.options());
    String configs = tableTemplateApi.list().stream()
        .map(x -> x.getSpec())
        .filter(x -> x.getDatabases() == null || x.getDatabases().contains(source.database()))
        .filter(x -> x.getMethods() == null || x.getMethods().contains(K8sUtils.method(source)))
        .filter(x -> x.getConnector() != null)
        .map(x -> x.getConnector())
        .map(x -> new Template.SimpleTemplate(x).render(env)).collect(Collectors.joining("\n"));
    Properties props = new Properties();
    try {
      props.load(new StringReader(configs));
    } catch (IOException e) {
      throw new SQLException(e);
    }
    Map<String, String> map = new HashMap<>();
    for (String key : props.stringPropertyNames()) {
      map.put(key, props.getProperty(key));
    }
    return map;
  }
}
