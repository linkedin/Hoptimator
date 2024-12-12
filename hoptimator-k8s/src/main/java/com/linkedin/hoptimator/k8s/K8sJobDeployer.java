package com.linkedin.hoptimator.k8s;

import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.linkedin.hoptimator.Job;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;
import com.linkedin.hoptimator.util.Template;


/** Specifies an abstract Job with concrete YAML by applying JobTemplates. */
class K8sJobDeployer extends K8sYamlDeployer<Job> {

  private final K8sApi<V1alpha1JobTemplate, V1alpha1JobTemplateList> jobTemplateApi;

  K8sJobDeployer(K8sContext context) {
    super(context);
    this.jobTemplateApi = new K8sApi<>(context, K8sApiEndpoints.JOB_TEMPLATES);
  }

  @Override
  public List<String> specify(Job job) throws SQLException {
    Function<SqlDialect, String> sql = job.sql();
    Template.Environment env = Template.Environment.EMPTY.with("name",
            job.sink().database() + "-" + job.sink().table().toLowerCase(Locale.ROOT))
        .with("database", job.sink().database())
        .with("schema", job.sink().schema())
        .with("table", job.sink().table())
        .with("sql", sql.apply(SqlDialect.ANSI))
        .with("flinksql", sql.apply(SqlDialect.FLINK))
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
