package com.linkedin.hoptimator.operator.subscription;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;
import io.kubernetes.client.extended.controller.reconciler.Request;
import io.kubernetes.client.extended.controller.reconciler.Result;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

import com.linkedin.hoptimator.Pipeline;
import com.linkedin.hoptimator.Source;
import com.linkedin.hoptimator.SqlDialect;
import com.linkedin.hoptimator.k8s.K8sApi;
import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1Pipeline;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineList;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1PipelineStatus;
import com.linkedin.hoptimator.k8s.models.V1alpha1Subscription;
import com.linkedin.hoptimator.k8s.models.V1alpha1SubscriptionList;
import com.linkedin.hoptimator.k8s.models.V1alpha1SubscriptionSpec;
import com.linkedin.hoptimator.k8s.models.V1alpha1SubscriptionStatus;
import com.linkedin.hoptimator.util.DeploymentService;
import com.linkedin.hoptimator.util.planner.PipelineRel;


/**
 * Reconciles Subscription CRs by planning SQL pipelines and deploying them
 * as Pipeline CRs with associated K8s resources.
 *
 * <p>This uses the new planner infrastructure (DeploymentService/PipelineRel)
 * rather than the legacy HoptimatorPlanner, providing a migration path for
 * existing Subscription-based workloads.
 */
public final class SubscriptionReconciler implements Reconciler {
  private static final Logger log = LoggerFactory.getLogger(SubscriptionReconciler.class);

  private final K8sContext context;
  private final K8sApi<V1alpha1Subscription, V1alpha1SubscriptionList> subscriptionApi;
  private final K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi;
  private final Planner planner;

  /**
   * Encapsulates the SQL planning step so it can be mocked in tests.
   */
  public interface Planner {
    PlanResult plan(String sql, String database, String name, Map<String, String> hints) throws Exception;
  }

  /**
   * The result of planning a Subscription's SQL.
   */
  public static class PlanResult {
    final String pipelineSql;
    final List<String> pipelineSpecs;

    PlanResult(String pipelineSql, List<String> pipelineSpecs) {
      this.pipelineSql = pipelineSql;
      this.pipelineSpecs = pipelineSpecs;
    }
  }

  private SubscriptionReconciler(K8sContext context, Planner planner) {
    this(context,
        new K8sApi<>(context, K8sApiEndpoints.SUBSCRIPTIONS),
        new K8sApi<>(context, K8sApiEndpoints.PIPELINES),
        planner);
  }

  SubscriptionReconciler(K8sContext context,
      K8sApi<V1alpha1Subscription, V1alpha1SubscriptionList> subscriptionApi,
      K8sApi<V1alpha1Pipeline, V1alpha1PipelineList> pipelineApi,
      Planner planner) {
    this.context = context;
    this.subscriptionApi = subscriptionApi;
    this.pipelineApi = pipelineApi;
    this.planner = planner;
  }

  @Override
  public Result reconcile(Request request) {
    log.info("Reconciling request {}", request);
    String name = request.getName();
    String namespace = request.getNamespace();

    Result result = new Result(true, pendingRetryDuration());
    try {
      V1alpha1Subscription object;
      try {
        object = subscriptionApi.get(namespace, name);
      } catch (SQLException e) {
        if (e.getErrorCode() == 404) {
          log.info("Object {}/{} deleted. Skipping.", namespace, name);
          return new Result(false);
        }
        throw e;
      }

      V1alpha1SubscriptionStatus status = object.getStatus();
      if (status == null) {
        status = new V1alpha1SubscriptionStatus();
        object.setStatus(status);
      }

      if (Objects.requireNonNull(object.getSpec()).getHints() == null) {
        object.getSpec().setHints(new HashMap<>());
      }

      if (diverged(object.getSpec(), status)) {
        // Phase 1: Plan the pipeline
        log.info("Planning a new pipeline for Subscription/{} with SQL `{}`...", name, object.getSpec().getSql());

        try {
          PlanResult plan = planner.plan(
              object.getSpec().getSql(),
              object.getSpec().getDatabase(),
              name,
              object.getSpec().getHints());

          status.setSql(object.getSpec().getSql());
          status.setHints(object.getSpec().getHints());
          status.setResources(plan.pipelineSpecs);
          status.setReady(false);
          status.setFailed(false);
          status.setMessage("Planned.");
        } catch (Exception e) {
          log.error("Encountered error when planning a pipeline for Subscription/{} with SQL `{}`.", name,
              object.getSpec().getSql(), e);
          status.setFailed(true);
          status.setMessage("Error: " + e.getMessage());
          result = new Result(true, failureRetryDuration());
        }
      } else if (!Boolean.TRUE.equals(status.getReady())) {
        // Phase 2/3: Deploy and monitor the Pipeline
        log.info("Checking status of pipeline for Subscription/{}...", name);

        V1alpha1Pipeline pipeline = pipelineApi.getIfExists(namespace, name);
        if (pipeline == null) {
          // Phase 2: Deploy the Pipeline CR
          log.info("Deploying pipeline for Subscription/{}...", name);
          try {
            deployPipeline(object);
            status.setReady(false);
            status.setFailed(false);
            status.setMessage("Deployed.");
          } catch (Exception e) {
            log.error("Encountered error deploying pipeline for Subscription/{}.", name, e);
            status.setFailed(true);
            status.setMessage("Error: " + e.getMessage());
            result = new Result(true, failureRetryDuration());
          }
        } else {
          // Phase 3: Check Pipeline readiness
          V1alpha1PipelineStatus pipelineStatus = pipeline.getStatus();
          if (pipelineStatus != null && Boolean.TRUE.equals(pipelineStatus.getReady())) {
            status.setReady(true);
            status.setFailed(false);
            status.setMessage("Ready.");
            log.info("Subscription/{} is ready.", name);
            result = new Result(false);
          } else if (pipelineStatus != null && Boolean.TRUE.equals(pipelineStatus.getFailed())) {
            status.setReady(false);
            status.setFailed(true);
            status.setMessage("Pipeline failed: " + pipelineStatus.getMessage());
            log.info("Pipeline for Subscription/{} failed.", name);
          } else {
            status.setReady(false);
            status.setFailed(false);
            status.setMessage("Deployed.");
            log.info("Pipeline for Subscription/{} is NOT ready.", name);
          }
        }
      } else {
        // Already ready — no-op
        result = new Result(false);
      }

      subscriptionApi.updateStatus(object, object.getStatus());
    } catch (Exception e) {
      log.error("Encountered exception while reconciling Subscription {}/{}", namespace, name, e);
      return new Result(true, failureRetryDuration());
    }
    return result;
  }

  private void deployPipeline(V1alpha1Subscription object) throws SQLException {
    String name = Objects.requireNonNull(object.getMetadata()).getName();
    String namespace = object.getMetadata().getNamespace();
    List<String> specs = object.getStatus().getResources();
    if (specs == null || specs.isEmpty()) {
      throw new SQLException("No pipeline specs to deploy for Subscription/" + name);
    }

    String sql = object.getStatus().getSql();

    V1alpha1Pipeline pipeline = new V1alpha1Pipeline()
        .kind(K8sApiEndpoints.PIPELINES.kind())
        .apiVersion(K8sApiEndpoints.PIPELINES.apiVersion())
        .metadata(new V1ObjectMeta().name(name).namespace(namespace))
        .spec(new V1alpha1PipelineSpec()
            .sql(sql)
            .yaml(String.join("\n---\n", specs)));

    pipelineApi.update(pipeline);
  }

  private static boolean diverged(V1alpha1SubscriptionSpec spec, V1alpha1SubscriptionStatus status) {
    return status.getSql() == null || !status.getSql().equals(spec.getSql())
        || status.getHints() == null || !status.getHints().equals(spec.getHints());
  }

  // TODO load from configuration
  Duration failureRetryDuration() {
    return Duration.ofMinutes(5);
  }

  // TODO load from configuration
  Duration pendingRetryDuration() {
    return Duration.ofMinutes(1);
  }

  /**
   * Creates a Planner that uses DeploymentService and a JDBC connection for SQL planning.
   */
  public static Planner jdbcPlanner(java.sql.Connection connection) {
    return (sql, database, name, hints) -> {
      com.linkedin.hoptimator.jdbc.HoptimatorConnection hoptConn =
          (com.linkedin.hoptimator.jdbc.HoptimatorConnection) connection;

      // Set up connection properties with hints
      Properties props = new Properties();
      props.putAll(hoptConn.connectionProperties());
      if (hints != null && !hints.isEmpty()) {
        String hintsStr = hints.entrySet().stream()
            .map(e -> java.net.URLEncoder.encode(e.getKey(), java.nio.charset.StandardCharsets.UTF_8)
                + "=" + java.net.URLEncoder.encode(e.getValue(), java.nio.charset.StandardCharsets.UTF_8))
            .collect(java.util.stream.Collectors.joining(","));
        props.put("hints", hintsStr);
      }
      String pipelineName = database + "-" + name;
      props.setProperty(DeploymentService.PIPELINE_OPTION, pipelineName);

      // Plan the SQL query
      org.apache.calcite.jdbc.CalcitePrepare.Context calciteContext = hoptConn.createPrepareContext();
      org.apache.calcite.rel.RelRoot root =
          new com.linkedin.hoptimator.jdbc.HoptimatorDriver.Prepare(hoptConn).convert(calciteContext, sql).root;
      PipelineRel.Implementor plan = DeploymentService.plan(root, hoptConn.materializations(), props);

      // Set the sink based on the database and subscription name
      org.apache.calcite.jdbc.CalciteSchema rootSchema = calciteContext.getRootSchema();
      org.apache.calcite.jdbc.CalciteSchema dbSchema = rootSchema.getSubSchema(database, true);
      if (dbSchema == null || !(dbSchema.schema instanceof com.linkedin.hoptimator.Database)) {
        throw new SQLException(database + " is not a physical database.");
      }
      String dbName = ((com.linkedin.hoptimator.Database) dbSchema.schema).databaseName();
      List<String> sinkPath = new ArrayList<>(dbSchema.path(null));
      sinkPath.add(name);
      plan.setSink(dbName, sinkPath, root.rel.getRowType(), Collections.emptyMap());

      // Build Pipeline and collect specs
      Pipeline pipeline = plan.pipeline(name, connection);

      List<String> specs = new ArrayList<>();
      for (Source source : pipeline.sources()) {
        specs.addAll(DeploymentService.specify(source, connection));
      }
      specs.addAll(DeploymentService.specify(pipeline.sink(), connection));
      specs.addAll(DeploymentService.specify(pipeline.job(), connection));

      String pipelineSql = pipeline.job().sql().apply(SqlDialect.ANSI);
      return new PlanResult(pipelineSql, specs);
    };
  }

  public static Controller controller(K8sContext context, Planner planner) {
    Reconciler reconciler = new SubscriptionReconciler(context, planner);
    return ControllerBuilder.defaultBuilder(context.informerFactory())
        .withReconciler(reconciler)
        .withName("subscription-controller")
        .withWorkerCount(1)
        .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1Subscription.class, x).build())
        .build();
  }
}
