package com.linkedin.hoptimator.operator.subscription;

import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.catalog.HopTable;
import com.linkedin.hoptimator.models.V1alpha1Subscription;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionSpec;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionStatus;
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

import java.util.ArrayList;
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

    Result result = new Result(true, operator.pendingRetryDuration());
    try {
      V1alpha1Subscription object = operator.<V1alpha1Subscription>fetch(SUBSCRIPTION, namespace,
        name);
 
      if (object ==  null) {
        log.info("Object {}/{} deleted, skipping.", namespace, name);
        return new Result(false);
      }
      
      String kind = object.getKind();

      V1alpha1SubscriptionStatus status = object.getStatus();
      if (status == null) {
        status = new V1alpha1SubscriptionStatus();
        object.setStatus(status);
      }

      // We deploy in three phases:
      // 1. Plan a pipeline, and write the plan to Status.
      // 2. Deploy the pipeline per plan.
      // 3. Verify readiness of the entire pipeline.
      // Each phase should be a separate reconcilation loop to avoid races.
      // TODO: We should disown orphaned resources when the pipeline changes.
      if (status.getSql() == null || !status.getSql().equals(object.getSpec().getSql())) {
        // Phase 1
        log.info("Planning a new pipeline for {}/{} with SQL `{}`...", kind, name, object.getSpec().getSql());

        Pipeline pipeline = pipeline(object);
        status.setResources(pipeline.resources().stream()
          .map(x -> x.render(templateFactory))
          .collect(Collectors.toList()));

        status.setSql(object.getSpec().getSql());
        status.setReady(null);  // null indicates that pipeline needs to be deployed
        status.setMessage("Planned.");
      } else if (status.getReady() == null && status.getResources() != null) {
        // Phase 2
        log.info("Deploying pipeline for {}/{}...", kind, name);

        boolean deployed = status.getResources().stream()
          .allMatch(x -> apply(x, object));

        if (deployed) {
          status.setReady(false);
          status.setMessage("Deployed.");
        } else {
          return new Result(true, operator.failureRetryDuration());
        }
      } else {
        log.info("Checking status of pipeline for {}/{}...", kind, name);

        boolean ready = status.getResources().stream()
          .allMatch(x -> checkStatus(x));

        if (ready) {
          status.setReady(true);
          status.setMessage("Ready.");
          log.info("{}/{} is ready.", kind, name);
          result = new Result(false);
        } else {
          status.setReady(false);
          status.setMessage("Deployed.");
          log.info("Pipeline for {}/{} is NOT ready.", kind, name);
        }
      }

      operator.apiFor(SUBSCRIPTION).updateStatus(object, x -> object.getStatus())
        .onFailure((x, y) -> log.error("Failed to update status of {}/{}: {}.", kind, name, y.getMessage()));
    } catch (Exception e) {
      log.error("Encountered exception while reconciling Subscription {}/{}", namespace, name, e);
      return new Result(true, operator.failureRetryDuration());
    }
    return result;
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
    log.info("Implementing sink table {}.{} with {} resources.", database, name, sink.writeResources().size());
    impl.implement(sink);

    return impl.pipeline(sink);
  }

  private boolean apply(String yaml, V1alpha1Subscription owner) {
    V1OwnerReference ownerReference = new V1OwnerReference();
    ownerReference.kind(owner.getKind());
    ownerReference.name(owner.getMetadata().getName());
    ownerReference.apiVersion(owner.getApiVersion());
    ownerReference.uid(owner.getMetadata().getUid());

    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    String namespace = obj.getMetadata().getNamespace();
    String name = obj.getMetadata().getName();
    KubernetesApiResponse<DynamicKubernetesObject> existing = operator.apiFor(obj).get(namespace, name);
    if (existing.isSuccess()) {
      String resourceVersion = existing.getObject().getMetadata().getResourceVersion();
      log.info("Updating existing downstream resource {}/{} {} as \n{}",
        namespace, name, resourceVersion, yaml);
      List<V1OwnerReference> owners = existing.getObject().getMetadata().getOwnerReferences();
      if (owners == null) {
        owners = new ArrayList<>();
      }
      if (owners.stream().anyMatch(x -> x.getUid().equals(ownerReference.getUid()))) {
        log.info("Existing downstream resource {}/{} is already owned by {}/{}.",
          namespace, name, ownerReference.getKind(), ownerReference.getName());
      } else {
        log.info("Existing downstream resource {}/{} will be owned by {}/{} and {} others.",
          namespace, name, ownerReference.getKind(), ownerReference.getName(), owners.size());
        owners.add(ownerReference);
      }
      obj.setMetadata(obj.getMetadata().ownerReferences(owners).resourceVersion(resourceVersion));
      KubernetesApiResponse<DynamicKubernetesObject> response = operator.apiFor(obj).update(obj);
      if (!response.isSuccess()) {
        log.error("Error updating downstream resource {}/{}: {}.", namespace, name, response.getStatus().getMessage());
        return false;
      }
    } else {
      log.info("Creating downstream resource {}/{} as \n{}", namespace, name, yaml);
      obj.setMetadata(obj.getMetadata().addOwnerReferencesItem(ownerReference));
      KubernetesApiResponse<DynamicKubernetesObject> response = operator.apiFor(obj).create(obj);
      if (!response.isSuccess()) {
        log.error("Error creating downstream resource {}/{}: {}.", namespace, name, response.getStatus().getMessage());
        return false;
      }
    }
    return true;
  }

  private boolean checkStatus(String yaml) {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    String namespace = obj.getMetadata().getNamespace();
    String name = obj.getMetadata().getName();
    String kind = obj.getKind();
    try {
      KubernetesApiResponse<DynamicKubernetesObject> existing = operator.apiFor(obj).get(namespace, name);
      existing.onFailure((code, status) -> log.warn("Failed to fetch {}/{}: {}.", kind, name, status.getMessage()));
      if (!existing.isSuccess()) {
        return false;
      }
      if (isReady(existing.getObject())) {
        log.info("{}/{} is ready.", kind, name);
        return true;
      } else {
        log.info("{}/{} is NOT ready.", kind, name);
        return false;
      }
    } catch (Exception e) {
      return false;
    }
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

