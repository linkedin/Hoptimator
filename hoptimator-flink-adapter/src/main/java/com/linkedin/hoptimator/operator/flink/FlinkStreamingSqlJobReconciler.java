package com.linkedin.hoptimator.operator.flink;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.catalog.flink.FlinkStreamingSqlJob;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.K8sYamlApi;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJob;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJobList;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJobSpec.DialectEnum;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJobSpec.ExecutionModeEnum;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJobStatus;

import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Manifests streaming SqlJobs as Flink session jobs.
 */
public class FlinkStreamingSqlJobReconciler implements Reconciler {
  private static final Logger log = LoggerFactory.getLogger(FlinkStreamingSqlJobReconciler.class);

  private final K8sContext context;
  private final K8sApi<V1alpha1SqlJob, V1alpha1SqlJobList> sqlJobApi;
  private final K8sYamlApi yamlApi;

  public FlinkStreamingSqlJobReconciler(K8sContext context) {
    this.context = context;
    this.sqlJobApi = new K8sApi<>(context, FlinkControllerProvider.SQL_JOBS);
    this.yamlApi = new K8sYamlApi(context);
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    try {
      V1alpha1SqlJob object;
      try {
        object = sqlJobApi.get(namespace, name);
      } catch (SQLException e) {
        if (e.getErrorCode() == 404) {
          log.info("Object {} deleted. Skipping.", name);
          return new Result(false);
        }
        throw e;
      }

      V1alpha1SqlJobStatus status = object.getStatus();
      if (status == null) {
        status = new V1alpha1SqlJobStatus();
        object.setStatus(status);
      }

      DialectEnum dialect = object.getSpec().getDialect();
      if (!DialectEnum.FLINK.equals(dialect)) {
        log.info("Not Flink SQL. Skipping.");
        return new Result(false);
      }

      ExecutionModeEnum mode = object.getSpec().getExecutionMode();
      if (!ExecutionModeEnum.STREAMING.equals(mode)) {
        log.info("Not a streaming job. Skipping.");
        return new Result(false);
      }

      List<String> sql = object.getSpec().getSql();
      String script = sql.stream().collect(Collectors.joining(";\n"));
      Map<String, String> files = object.getSpec().getFiles();

      Resource.TemplateFactory templateFactory = new Resource.SimpleTemplateFactory(Resource.Environment.EMPTY);
      Resource sqlJob = new FlinkStreamingSqlJob(namespace, name, script, files);

      for (String yaml : sqlJob.render(templateFactory)) {
        DynamicKubernetesObject obj = yamlApi.objFromYaml(yaml);
        context.own(obj);
        yamlApi.update(obj);
      }

      object.getStatus().setReady(true);
      object.getStatus().setMessage("Ready.");
      sqlJobApi.updateStatus(object, object.getStatus());
    } catch (Exception e) {
      log.error("Encountered exception while reconciling Flink streaming SqlJob {}/{}", namespace, name, e);
      return new Result(true, Duration.ofMinutes(5));
    }
    return new Result(false);
  }
}
