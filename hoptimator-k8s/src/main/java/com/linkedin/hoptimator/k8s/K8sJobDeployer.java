package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateSpec;
import com.linkedin.hoptimator.util.ConfigService;
import com.linkedin.hoptimator.util.Template;


/** Specifies an abstract Job with concrete YAML by applying JobTemplates. */
class K8sJobDeployer extends K8sYamlDeployer {

  private static final String FLINK_CONFIG = "flink.config";

  private final K8sContext context;
  private final Job job;
  private final K8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> jobTemplateApi;

  K8sJobDeployer(Job job, K8sContext context) {
    super(context);
    this.context = context;
    this.job = job;
    this.jobTemplateApi = new K8sApi<>(context, K8sApiEndpoints.JOB_TEMPLATES);
  }

  @Override
  public List<String> specify() throws SQLException {
    Properties properties = ConfigService.config(context.connection(), false, FLINK_CONFIG);
    properties.putAll(job.sink().options());
    Function<SqlDialect, String> sql = job.sql();
    String name = K8sUtils.canonicalizeName(job.sink().database(), job.name());
    Template.Environment env = new Template.SimpleEnvironment()
        .with("name", name)
        .with("database", job.sink().database())
        .with("schema", job.sink().schema())
        .with("table", job.sink().table())
        .with("sql", sql.apply(SqlDialect.ANSI))
        .with("flinksql", sql.apply(SqlDialect.FLINK))
        .with("flinkconfigs", properties)
        .with(job.sink().options());
    return jobTemplateApi.list()
        .stream()
        .map(V1alpha1JobTemplate::getSpec)
        .filter(Objects::nonNull)
        .filter(x -> x.getDatabases() == null || x.getDatabases().contains(job.sink().database()))
        .map(V1alpha1JobTemplateSpec::getYaml)
        .filter(Objects::nonNull)
        .map(x -> new Template.SimpleTemplate(x).render(env))
        .collect(Collectors.toList());
  }
}
