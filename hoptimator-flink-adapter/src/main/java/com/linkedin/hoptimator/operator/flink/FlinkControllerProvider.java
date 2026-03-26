package com.linkedin.hoptimator.operator.flink;

import com.linkedin.hoptimator.k8s.K8sApiEndpoint;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJob;
import com.linkedin.hoptimator.k8s.models.V1alpha1SqlJobList;
import com.linkedin.hoptimator.operator.ControllerProvider;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;


/** A bridge to flink-kubernetes-operator */
public class FlinkControllerProvider implements ControllerProvider {

  public static final K8sApiEndpoint<V1alpha1SqlJob, V1alpha1SqlJobList> SQL_JOBS =
      new K8sApiEndpoint<>("SqlJob", "hoptimator.linkedin.com", "v1alpha1", "sqljobs", false,
          V1alpha1SqlJob.class, V1alpha1SqlJobList.class);

  @Override
  public Collection<Controller> controllers(K8sContext context) {
    context.registerInformer(SQL_JOBS, Duration.ofMinutes(5));

    Reconciler reconciler = new FlinkStreamingSqlJobReconciler(context);
    Controller controller = ControllerBuilder.defaultBuilder(context.informerFactory())
        .withReconciler(reconciler)
        .withName("flink-streaming-sql-job-controller")
        .withWorkerCount(1)
        .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1SqlJob.class, x).build())
        .build();

    return Collections.singleton(controller);
  }
}
