package com.linkedin.hoptimator.k8s;

import com.linkedin.hoptimator.util.Job;
import com.linkedin.hoptimator.util.Template;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplate;
import com.linkedin.hoptimator.k8s.models.V1alpha1JobTemplateList;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.sql.SQLException;

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
    Template.Environment env = Template.Environment.EMPTY
        .with("name", job.sink().database() + "-" + job.sink().table().toLowerCase(Locale.ROOT))
        .with("database", job.sink().database())
        .with("schema", job.sink().schema())
        .with("table", job.sink().table())
        .with("sql", sql.apply(MysqlSqlDialect.DEFAULT))
        .with("ansisql", sql.apply(AnsiSqlDialect.DEFAULT))
        .with("calcitesql", sql.apply(CalciteSqlDialect.DEFAULT))
        .with(job.sink().options());
    return jobTemplateApi.list().stream()
        .map(x -> x.getSpec())
        .filter(x -> x.getDatabases() == null || x.getDatabases().contains(job.sink().database()))
        .filter(x -> x.getYaml() != null)
        .map(x -> x.getYaml())
        .map(x -> new Template.SimpleTemplate(x).render(env))
        .collect(Collectors.toList());
  }
}
