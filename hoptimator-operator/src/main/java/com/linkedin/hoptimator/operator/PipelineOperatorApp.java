package com.linkedin.hoptimator.operator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;

import com.linkedin.hoptimator.k8s.K8sApiEndpoints;
import com.linkedin.hoptimator.k8s.K8sContext;
import com.linkedin.hoptimator.operator.pipeline.PipelineReconciler;


public class PipelineOperatorApp {
  private static final Logger log = LoggerFactory.getLogger(HoptimatorOperatorApp.class);

  public static void main(String[] args) throws Exception {
    new PipelineOperatorApp().run();
  }

  public void run() throws Exception {
    K8sContext context = K8sContext.currentContext();

    // register informers
    context.registerInformer(K8sApiEndpoints.PIPELINES, Duration.ofMinutes(5));

    List<Controller> controllers = new ArrayList<>();
    // TODO: add additional controllers from ControllerProvider SPI
    controllers.add(PipelineReconciler.controller(context));

    ControllerManager controllerManager =
        new ControllerManager(context.informerFactory(), controllers.toArray(new Controller[0]));

    log.info("Starting operator with {} controllers.", controllers.size());
    controllerManager.run();
  }
}
