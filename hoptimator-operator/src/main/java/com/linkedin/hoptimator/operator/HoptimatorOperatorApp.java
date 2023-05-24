package com.linkedin.hoptimator.operator;

import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;

import com.linkedin.hoptimator.models.V1alpha1Subscription;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionList;
import com.linkedin.hoptimator.models.V1alpha1SqlJob;
import com.linkedin.hoptimator.models.V1alpha1SqlJobList;
import com.linkedin.hoptimator.operator.subscription.SubscriptionReconciler;
import com.linkedin.hoptimator.planner.HoptimatorPlanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HoptimatorOperatorApp {

  final String modelPath;
  final String namespace;
  final Properties properties;

  /** This constructor is likely to evolve and break. */
  public HoptimatorOperatorApp(String modelPath, String namespace, Properties properties) {
    this.modelPath = modelPath;
    this.namespace = namespace;
    this.properties = properties;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Missing model file argument.");
    }
    new HoptimatorOperatorApp(args[0], "default", new Properties()).run();
  }

  public void run() throws Exception {
    HoptimatorPlanner.Factory plannerFactory = HoptimatorPlanner.Factory.fromModelFile(modelPath,
        properties);

    // ensure model file works, and that static classes are initialized in the main thread
    HoptimatorPlanner planner = plannerFactory.makePlanner();

    ApiClient apiClient = Config.defaultClient();
    apiClient.setHttpClient(apiClient.getHttpClient().newBuilder()
      .readTimeout(0, TimeUnit.SECONDS).build());
    SharedInformerFactory informerFactory = new SharedInformerFactory(apiClient);
    Operator operator = new Operator(namespace, apiClient, informerFactory, properties);
    // TODO replace hard-coded "default" namespace with command-line argument

    operator.registerApi("SqlJob", "sqljob", "sqljobs",
      "hoptimator.linkedin.com", "v1alpha1", V1alpha1SqlJob.class, V1alpha1SqlJobList.class);
 
    operator.registerApi("Subscription", "subscription", "subscriptions", "hoptimator.linkedin.com",
      "v1alpha1", V1alpha1Subscription.class, V1alpha1SubscriptionList.class);

    List<Controller> controllers = new ArrayList<>();
    controllers.addAll(ControllerService.controllers(operator));
    controllers.add(SubscriptionReconciler.controller(operator, plannerFactory));

    ControllerManager controllerManager = new ControllerManager(operator.informerFactory(),
      controllers.toArray(new Controller[0]));
  
    controllerManager.run();  
  }
}
