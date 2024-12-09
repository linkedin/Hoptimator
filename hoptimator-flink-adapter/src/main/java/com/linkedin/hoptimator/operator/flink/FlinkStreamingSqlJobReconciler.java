package com.linkedin.hoptimator.operator.flink;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.catalog.flink.FlinkStreamingSqlJob;
import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.models.V1alpha1SqlJob;
import com.linkedin.hoptimator.models.V1alpha1SqlJobSpec.DialectEnum;
import com.linkedin.hoptimator.models.V1alpha1SqlJobSpec.ExecutionModeEnum;
import com.linkedin.hoptimator.models.V1alpha1SqlJobStatus;

import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;


/**
 * Manifests streaming SqlJobs as Flink jobs.
 *
 */
public class FlinkStreamingSqlJobReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(FlinkStreamingSqlJobReconciler.class);
  private final static String SQLJOB = "hoptimator.linkedin.com/v1alpha1/SqlJob";

  private final Operator operator;

  public FlinkStreamingSqlJobReconciler(Operator operator) {
    this.operator = operator;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();
    Result result = new Result(true, operator.pendingRetryDuration());

    try {
      V1alpha1SqlJob object = operator.<V1alpha1SqlJob>fetch(SQLJOB, namespace, name);

      if (object == null) {
        log.info("Object {}/{} deleted. Skipping.");
        return new Result(false);
      }

      V1alpha1SqlJobStatus status = object.getStatus();
      if (status == null) {
        status = new V1alpha1SqlJobStatus();
        object.setStatus(status);
      }

      List<String> sql = object.getSpec().getSql();
      String script = sql.stream().collect(Collectors.joining(";\n"));

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

      Resource.TemplateFactory templateFactory = new Resource.SimpleTemplateFactory(Resource.Environment.EMPTY);
      Resource sqlJob = new FlinkStreamingSqlJob(namespace, name, script);
      boolean allReady = true;
      boolean anyFailed = false;
      for (String yaml : sqlJob.render(templateFactory)) {
        operator.apply(yaml, object);
        if (!operator.isReady(yaml)) {
          allReady = false;
        }
        if (operator.isFailed(yaml)) {
          anyFailed = true;
          allReady = false;
        }
      }

      object.getStatus().setReady(allReady);
      object.getStatus().setFailed(anyFailed);

      if (allReady) {
        object.getStatus().setMessage("Ready.");
        result = new Result(false); // done
      }
      if (anyFailed) {
        object.getStatus().setMessage("Failed.");
        result = new Result(false); // done
      }

      operator.apiFor(SQLJOB)
          .updateStatus(object, x -> object.getStatus())
          .onFailure((x, y) -> log.error("Failed to update status of SqlJob {}: {}.", name, y.getMessage()))
          .throwsApiException();
    } catch (Exception e) {
      log.error("Encountered exception while reconciling Flink streaming SqlJob {}/{}", namespace, name, e);
      return new Result(true, operator.failureRetryDuration());
    }
    return result;
  }
}

