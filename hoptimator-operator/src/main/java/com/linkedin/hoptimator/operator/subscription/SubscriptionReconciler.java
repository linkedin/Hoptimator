package com.linkedin.hoptimator.operator.subscription;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.catalog.HopTable;
import com.linkedin.hoptimator.models.V1alpha1Subscription;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionSpec;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionStatus;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionStatusResources;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionStatusResourceRef;
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
import java.util.stream.Collectors;

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
  
      if (object == null) {
        log.info("Object {}/{}/{} deleted. Skipping.", namespace, SUBSCRIPTION, name);
        return new Result(false);
      }
 
      String kind = object.getKind();

      V1alpha1SubscriptionStatus status = object.getStatus();
      if (status == null) {
        status = new V1alpha1SubscriptionStatus();
        status.setReady(false);
        status.setMessage("Deploying...");
      }
      object.setStatus(status);
      log.info("Status message: {}", status.getMessage());
      
      V1alpha1SubscriptionSpec spec = object.getSpec();

      if (status.getSql() != null && spec.getSql().equals(status.getSql()) && status.getResources() != null) {
        // just report on status and exit
        log.info("Checking status of existing pipeline with {} resources...", status.getResources().size());
        long readyResourcesCount = status.getResources().stream()
          .map(x -> x.getResourceRef())
          .map(x -> operator.apiFor(x.getApiVersion(), x.getKind()).get(namespace, x.getName()))
          .filter(x -> x.isSuccess())
          .map(x -> x.getObject())
          .filter(x -> isReady(x))
          .count();
        if (readyResourcesCount != status.getResources().size()) {
          log.info("So far, {} out of {} child resources are ready.", readyResourcesCount,
            status.getResources().size());
          return new Result(true, operator.pendingRetryDuration());
        } else {
          log.info("All child resources are ready.");
          status.setReady(true);  // This should be the only place we set ready=true.
          status.setMessage("Ready!");
          operator.apiFor(SUBSCRIPTION).updateStatus(object, x -> object.getStatus());
          log.info("Done reconciling {}/{}", namespace, name);
          return new Result(false);
        }
      } else {
        // plan a new pipeline 
        Pipeline pipeline = pipeline(object);
        log.info("Planned pipeline requires {} child resources.", pipeline.resources().size());

        V1OwnerReference ownerReference = new V1OwnerReference();
        ownerReference.kind(object.getKind());
        ownerReference.name(object.getMetadata().getName());
        ownerReference.apiVersion(object.getApiVersion());
        ownerReference.uid(object.getMetadata().getUid());

        log.info("Applying resources...");
        List<DynamicKubernetesObject> applied = pipeline.resources().stream()
          .map(x -> operator.applyResource(x, ownerReference, templateFactory))
          .collect(Collectors.toList());

        if (applied.stream().anyMatch(x -> x == null)) {
          log.error("One or more downstream resources for {}/{}/{} were not created.",
            namespace, kind, name);
        }

        // Disown any orphaned child resources
        if (status.getResources() != null) {
          List<V1alpha1SubscriptionStatusResourceRef> orphanedResources = status.getResources()
            .stream().map(x -> x.getResourceRef())
            .filter(x -> applied.stream().noneMatch(y -> y != null
              && y.getMetadata().getName().equals(x.getName())
              && y.getKind().equals(x.getKind())))
            .collect(Collectors.toList());
          if (!orphanedResources.isEmpty()) {
            log.info("New plan orphans {} child resources. Disowning them. **TODO**",
              orphanedResources.size());
            // TODO Disown orphaned resources.
          }
        }

        // TODO keep any orphaned resources that we failed to disown.
        status.setResources(applied.stream()
          .filter(x -> x != null)
          .map(x -> new V1alpha1SubscriptionStatusResourceRef()
            .name(x.getMetadata().getName())
            .kind(x.getKind())
            .apiVersion(x.getApiVersion()))
          .map(x -> new V1alpha1SubscriptionStatusResources().resourceRef(x))
          .collect(Collectors.toList()));

        status.setSql(spec.getSql());
        operator.apiFor(SUBSCRIPTION).updateStatus(object, x -> object.getStatus());
        return new Result(true, operator.pendingRetryDuration());
      }
    } catch (Exception e) {
      log.error("Encountered exception while reconciling Subscription {}/{}", namespace, name, e);
      return new Result(true, operator.failureRetryDuration());
    }
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

  private static boolean isReady(DynamicKubernetesObject obj) {
    // We make a best effort to guess the status of the dynamic object. By default, it's ready.
    if (obj == null || obj.getRaw() == null) {
      return false;
    }
    try {
      return obj.getRaw().get("status").getAsJsonObject().get("ready").getAsBoolean();
    } catch (Exception e) {
      log.debug("Exception looking for .status.ready. Swallowing.", e);
    }
    try {
      return obj.getRaw().get("status").getAsJsonObject().get("state").getAsString()
        .matches("(?i)READY|RUNNING|FINISHED");
    } catch (Exception e) {
      log.debug("Exception looking for .status.state. Swallowing.", e);
    }
    try {
      return obj.getRaw().get("status").getAsJsonObject().get("jobStatus").getAsJsonObject()
        .get("state").getAsString().matches("(?i)READY|RUNNING|FINISHED");
    } catch (Exception e) {
      log.debug("Exception looking for .status.jobStatus.state. Swallowing.", e);
    }
    // TODO: Look for common Conditions
    log.warn("Resource {}/{}/{} considered ready by default.", obj.getMetadata().getNamespace(),
      obj.getKind(), obj.getMetadata().getName());
    return true;
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

