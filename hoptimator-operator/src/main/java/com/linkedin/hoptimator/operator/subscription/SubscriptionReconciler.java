package com.linkedin.hoptimator.operator.subscription;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesObject;
import io.kubernetes.client.util.generic.dynamic.Dynamics;

import com.linkedin.hoptimator.catalog.HopTable;
import com.linkedin.hoptimator.catalog.Resource;
import com.linkedin.hoptimator.models.V1alpha1Subscription;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionSpec;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionStatus;
import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.planner.HoptimatorPlanner;
import com.linkedin.hoptimator.planner.Pipeline;
import com.linkedin.hoptimator.planner.PipelineRel;


public final class SubscriptionReconciler implements Reconciler {
  private final static Logger log = LoggerFactory.getLogger(SubscriptionReconciler.class);
  private final static String SUBSCRIPTION = "hoptimator.linkedin.com/v1alpha1/Subscription";

  private final Operator operator;
  private final HoptimatorPlanner.Factory plannerFactory;
  private final Resource.Environment environment;
  private final Predicate<V1alpha1Subscription> filter;

  private SubscriptionReconciler(Operator operator, HoptimatorPlanner.Factory plannerFactory,
      Resource.Environment environment, Predicate<V1alpha1Subscription> filter) {
    this.operator = operator;
    this.plannerFactory = plannerFactory;
    this.environment = environment;
    this.filter = filter;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    Result result = new Result(true, operator.pendingRetryDuration());
    try {
      V1alpha1Subscription object = operator.fetch(SUBSCRIPTION, namespace, name);

      if (object == null) {
        log.info("Object {}/{} deleted. Skipping.", namespace, name);
        return new Result(false);
      }

      if (filter != null && !filter.test(object)) {
        log.info("Object {}/{} filtered. Skipping.", namespace, name);
        return new Result(false);
      }

      String kind = object.getKind();

      Objects.requireNonNull(object.getMetadata());
      object.getMetadata().setNamespace(namespace);

      V1alpha1SubscriptionStatus status = object.getStatus();
      if (status == null) {
        status = new V1alpha1SubscriptionStatus();
        object.setStatus(status);
      }

      Objects.requireNonNull(object.getSpec());
      if (object.getSpec().getHints() == null) {
        object.getSpec().setHints(new HashMap<>());
      }

      if (status.getJobResources() == null) {
        status.setJobResources(Collections.emptyList());
      }

      if (status.getDownstreamResources() == null) {
        status.setDownstreamResources(Collections.emptyList());
      }

      // We deploy in three phases:
      // 1. Plan a pipeline, and write the plan to Status.
      // 2. Deploy the pipeline per plan.
      // 3. Verify readiness of the entire pipeline.
      // Each phase should be a separate reconciliation loop to avoid races.
      // TODO: We should disown orphaned resources when the pipeline changes.
      if (diverged(object.getSpec(), status)) {
        // Phase 1
        log.info("Planning a new pipeline for {}/{} with SQL `{}`...", kind, name, object.getSpec().getSql());

        try {
          Pipeline pipeline = pipeline(object);
          Resource.Environment subEnv = new SubscriptionEnvironment(namespace, name, pipeline).orElse(environment);
          Resource.TemplateFactory templateFactory = new Resource.SimpleTemplateFactory(subEnv);

          // For sink resources, also expose hints.
          Resource.TemplateFactory sinkTemplateFactory = new Resource.SimpleTemplateFactory(
              subEnv.orElse(new Resource.SimpleEnvironment(map(object.getSpec().getHints()))));

          // Render resources related to all source tables.
          List<String> upstreamResources = pipeline.upstreamResources()
              .stream()
              .flatMap(x -> x.render(templateFactory).stream())
              .collect(Collectors.toList());

          // Render the SQL job
          Collection<String> sqlJob = pipeline.sqlJob().render(templateFactory);

          // Render resources related to the sink table. For these resources, we pass along any
          // "hints" as part of the environment.
          List<String> downstreamResources = pipeline.downstreamResources()
              .stream()
              .flatMap(x -> x.render(sinkTemplateFactory).stream())
              .collect(Collectors.toList());

          List<String> combined = new ArrayList<>();
          combined.addAll(upstreamResources);
          combined.addAll(sqlJob);
          combined.addAll(downstreamResources);

          status.setResources(combined);
          status.setJobResources(new ArrayList<>(sqlJob));
          status.setDownstreamResources(downstreamResources);

          status.setSql(object.getSpec().getSql());
          status.setHints(object.getSpec().getHints());
          status.setReady(null);  // null indicates that pipeline needs to be deployed
          status.setFailed(null);
          status.setMessage("Planned.");
        } catch (Exception e) {
          log.error("Encountered error when planning a pipeline for {}/{} with SQL `{}`.", kind, name,
              object.getSpec().getSql(), e);

          // Mark the Subscription as failed.
          status.setFailed(true);
          status.setMessage("Error: " + e.getMessage());
          result = new Result(true, operator.failureRetryDuration());
        }
      } else if (status.getReady() == null && status.getResources() != null) {
        // Phase 2
        log.info("Deploying pipeline for {}/{}...", kind, name);

        boolean deployed = status.getResources().stream().allMatch(x -> apply(x, object));

        if (deployed) {
          status.setReady(false);
          status.setFailed(false);
          status.setMessage("Deployed.");
        } else {
          return new Result(true, operator.failureRetryDuration());
        }
      } else {
        log.info("Checking status of pipeline for {}/{}...", kind, name);

        Objects.requireNonNull(status.getResources());
        boolean ready = status.getResources().stream().allMatch(operator::isReady);

        if (ready) {
          status.setReady(true);
          status.setFailed(false);
          status.setMessage("Ready.");
          log.info("{}/{} is ready.", kind, name);
          result = new Result(false);
        } else {
          status.setReady(false);
          status.setFailed(false);
          status.setMessage("Deployed.");
          log.info("Pipeline for {}/{} is NOT ready.", kind, name);
        }
      }

      status.setAttributes(Stream.concat(status.getJobResources().stream(), status.getDownstreamResources().stream())
          .map(this::fetchAttributes)
          .flatMap(x -> x.entrySet().stream())
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y)));

      operator.apiFor(SUBSCRIPTION)
          .updateStatus(object, x -> object.getStatus())
          .onFailure((x, y) -> log.error("Failed to update status of {}/{}: {}.", kind, name, y.getMessage()));
    } catch (Exception e) {
      log.error("Encountered exception while reconciling Subscription {}/{}", namespace, name, e);
      return new Result(true, operator.failureRetryDuration());
    }
    return result;
  }

  private boolean apply(String yaml, KubernetesObject owner) {
    try {
      operator.apply(yaml, owner);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private Pipeline pipeline(V1alpha1Subscription object) throws Exception {
    Objects.requireNonNull(object.getMetadata());
    Objects.requireNonNull(object.getSpec());
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

  // Whether status has diverged from spec (i.e. we need to re-plan the pipeline)
  private static boolean diverged(V1alpha1SubscriptionSpec spec, V1alpha1SubscriptionStatus status) {
    return status.getSql() == null || !status.getSql().equals(spec.getSql()) || status.getHints() == null
        || !status.getHints().equals(spec.getHints());
  }

  // Fetch attributes from downstream controllers
  private Map<String, String> fetchAttributes(String yaml) {
    DynamicKubernetesObject obj = Dynamics.newFromYaml(yaml);
    String namespace = obj.getMetadata().getNamespace();
    String name = obj.getMetadata().getName();
    String kind = obj.getKind();
    try {
      KubernetesApiResponse<DynamicKubernetesObject> existing = operator.apiFor(obj).get(namespace, name);
      existing.onFailure((code, status) -> log.info("Failed to fetch {}/{}: {}.", kind, name, status.getMessage()));
      if (!existing.isSuccess()) {
        return Collections.emptyMap();
      } else {
        return guessAttributes(existing.getObject());
      }
    } catch (Exception e) {
      return Collections.emptyMap();
    }
  }

  private static Map<String, String> guessAttributes(DynamicKubernetesObject obj) {
    // We make a best effort to guess the attributes of the dynamic object.
    if (obj == null || obj.getRaw() == null) {
      return Collections.emptyMap();
    }
    try {
      return obj.getRaw()
          .get("status")
          .getAsJsonObject()
          .get("attributes")
          .getAsJsonObject()
          .entrySet()
          .stream()
          .filter(x -> x.getValue().isJsonPrimitive())
          .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getAsString()));
    } catch (Exception e) {
      log.debug("Exception looking for .status.attributes. Swallowing.", e);
    }
    try {
      return obj.getRaw()
          .get("status")
          .getAsJsonObject()
          .get("jobStatus")
          .getAsJsonObject()
          .entrySet()
          .stream()
          .filter(x -> x.getValue().isJsonPrimitive())
          .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getAsString()));
    } catch (Exception e) {
      log.debug("Exception looking for .status.jobStatus. Swallowing.", e);
    }
    try {
      return obj.getRaw()
          .get("status")
          .getAsJsonObject()
          .entrySet()
          .stream()
          .filter(x -> x.getValue().isJsonPrimitive())
          .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getAsString()));
    } catch (Exception e) {
      log.debug("Exception looking for .status. Swallowing.", e);
    }
    return Collections.emptyMap();
  }

  public static Controller controller(Operator operator, HoptimatorPlanner.Factory plannerFactory,
      Resource.Environment environment, Predicate<V1alpha1Subscription> filter) {
    Reconciler reconciler = new SubscriptionReconciler(operator, plannerFactory, environment, filter);
    return ControllerBuilder.defaultBuilder(operator.informerFactory())
        .withReconciler(reconciler)
        .withName("subscription-controller")
        .withWorkerCount(1)
        .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1Subscription.class, x).build())
        .build();
  }

  private static Map<String, String> map(Map<String, String> m) {
    if (m == null) {
      return Collections.emptyMap();
    } else {
      return m;
    }
  }
}

