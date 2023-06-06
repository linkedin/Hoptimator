package com.linkedin.hoptimator.operator.flink;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.operator.ConfigAssembler;
import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.operator.RequestEnvironment;
import com.linkedin.hoptimator.models.V1alpha1SqlJob;

import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1OwnerReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FlinkSqlJobReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(FlinkSqlJobReconciler.class);
  private final static String SQLJOB = "hoptimator.linkedin.com/v1alpha1/SqlJob";

  private final Operator operator;

  public FlinkSqlJobReconciler(Operator operator) {
    this.operator = operator;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    try {
      V1alpha1SqlJob object = operator.<V1alpha1SqlJob>fetch(SQLJOB, namespace, name);
  
      if (object == null) {
        log.info("Object {}/{} deleted. Skipping.", namespace, name);
        return new Result(false);
      }

      V1OwnerReference ownerReference = new V1OwnerReference();
      ownerReference.kind(object.getKind());
      ownerReference.name(object.getMetadata().getName());
      ownerReference.apiVersion(object.getApiVersion());
      ownerReference.uid(object.getMetadata().getUid());
  
      List<String> sql = object.getSpec().getSql();

      RequestEnvironment env = new RequestEnvironment(request);
      Resource.TemplateFactory templateFactory = new Resource.SimpleTemplateFactory(env);
      FlinkDeployment deployment = new FlinkDeployment(sql);
      if (operator.applyResource(deployment, ownerReference, templateFactory) == null) {
        return new Result(true, operator.failureRetryDuration());
      }
    } catch (Exception e) {
      log.error("Encountered exception while reconciling KafkaTopic {}/{}", namespace, name, e);
      return new Result(true, operator.failureRetryDuration());
    }
    log.info("Done reconciling {}/{}", namespace, name);
    return new Result(false);
  }

  private static <T> List<T> list(List<T> maybeNull) {
    if (maybeNull == null) {
      return Collections.emptyList();
    } else {
      return maybeNull;
    }
  }

  private static <K, V> Map<K, V> map(Map<K, V> maybeNull) {
    if (maybeNull == null) {
      return Collections.emptyMap();
    } else {
      return maybeNull;
    }
  }
}

