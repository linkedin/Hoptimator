package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;
import com.linkedin.hoptimator.util.ConfigService;
import com.linkedin.hoptimator.util.Template;


/** Specifies an abstract Job with concrete YAML by applying JobTemplates. */
class K8sJobDeployer extends K8sYamlDeployer {

  private static final String FLINK_CONFIG = "flink.config";

  private final Properties connectionProperties;
  private final Job job;
  private final K8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> jobTemplateApi;

  K8sJobDeployer(Job job, K8sContext context, Properties connectionProperties) {
    super(context);
    this.connectionProperties = connectionProperties;
    this.job = job;
    this.jobTemplateApi = new K8sApi<>(context, K8sApiEndpoints.JOB_TEMPLATES);
  }

  @Override
  public List<String> specify() throws SQLException {
    Properties properties = ConfigService.config(this.connectionProperties, false, FLINK_CONFIG);
    properties.putAll(job.sink().options());
    Function<SqlDialect, String> sql = job.sql();
    String name = K8sUtils.canonicalizeName(job.sink().database(), job.name());
    Template.Environment env = new Template.SimpleEnvironment()
        .with("name", name)
        .with("database", job.sink().database())
        .with("schema", job.sink().schema())
        .with("table", job.sink().table())
        .with("pipelineName", job.sink().pipelineName())
        .with("sql", sql.apply(SqlDialect.ANSI))
        .with("flinksql", sql.apply(SqlDialect.FLINK))
        .with("flinkconfigs", properties)
        .with(job.sink().options());
    return jobTemplateApi.list()
        .stream()
        .map(x -> x.getSpec())
        .filter(x -> x.getDatabases() == null || x.getDatabases().contains(job.sink().database()))
        .filter(x -> x.getYaml() != null)
        .map(x -> x.getYaml())
        .map(x -> new Template.SimpleTemplate(x).render(env))
        .collect(Collectors.toList());
  }
}
