package com.linkedin.hoptimator.operator.subscription;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.catalog.HopTable;
import com.linkedin.hoptimator.models.V1alpha1Subscription;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionStatus;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionSpec;
import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.operator.ConfigAssembler;
import com.linkedin.hoptimator.operator.RequestEnvironment;
import com.linkedin.hoptimator.planner.HoptimatorPlanner;
import com.linkedin.hoptimator.planner.Pipeline;
import com.linkedin.hoptimator.planner.PipelineRel;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CountDownLatch;

public class SubscriptionReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(SubscriptionReconciler.class);
  private final static String SUBSCRIPTION = "hoptimator.linkedin.com/v1alpha1/Subscription";

  private final Operator operator;
  private final HoptimatorPlanner.Factory plannerFactory;

  private SubscriptionReconciler(Operator operator, HoptimatorPlanner.Factory plannerFactory) {
    this.operator = operator;
    this.plannerFactory = plannerFactory;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();
    RequestEnvironment env = new RequestEnvironment(request);
    Resource.TemplateFactory templateFactory = new Resource.SimpleTemplateFactory(env);

    try {
      V1alpha1Subscription object = operator.<V1alpha1Subscription>fetch(SUBSCRIPTION, namespace,
        name);
  
      if (object ==  null) {
        log.info("Object {}/{} deleted, skipping.", namespace, name);
        return new Result(false);
      }

      V1OwnerReference ownerReference = new V1OwnerReference();
      ownerReference.kind(object.getKind());
      ownerReference.name(object.getMetadata().getName());
      ownerReference.apiVersion(object.getApiVersion());
      ownerReference.uid(object.getMetadata().getUid());
   
      Pipeline pipeline = pipeline(object);
      boolean ready = pipeline.resources().stream()
        .map(x -> operator.applyResource(x, ownerReference, templateFactory)).allMatch(x -> x);

      V1alpha1SubscriptionStatus status = object.getStatus();
      if (status == null) {
        status = new V1alpha1SubscriptionStatus();
      }
      status.setReady(ready);

      operator.apiFor(SUBSCRIPTION).updateStatus(object, x -> object.getStatus());
    } catch (Exception e) {
      log.error("Encountered exception while reconciling Subscription {}/{}", namespace, name, e);
      return new Result(true, operator.failureRetryDuration());
    }
    log.info("Done reconciling {}/{}", namespace, name);
    return new Result(false);
  }

  Pipeline pipeline(V1alpha1Subscription object) throws Exception {
    String name = object.getMetadata().getName();
    String sql = object.getSpec().getSql();
    String database = object.getSpec().getDatabase();
    HoptimatorPlanner planner = plannerFactory.makePlanner();
    PipelineRel plan = planner.pipeline(sql);
    PipelineRel.Implementor impl = new PipelineRel.Implementor(plan);

    // Create an output/sink table using the subscription name, and add it to the pipeline.
    HopTable sink = planner.database(database).makeTable(name, impl.rowType());
    log.info("Implementing sink table {}.{} with {} resources.", database, name, sink.resources().size());
    impl.implement(sink);

    return impl.pipeline(sink);
  }

  public static Controller controller(Operator operator, HoptimatorPlanner.Factory plannerFactory) {
    Reconciler reconciler = new SubscriptionReconciler(operator, plannerFactory);
    return ControllerBuilder.defaultBuilder(operator.informerFactory())
      .withReconciler(reconciler)
      .withName("subscription-controller")
      .withWorkerCount(1)
      //.withReadyFunc(resourceInformer::hasSynced) // optional, only starts controller when the
      // cache has synced up
      //.withWorkQueue(resourceWorkQueue)
      //.watch()
      .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1Subscription.class, x).build())
      .build();
  }
}

