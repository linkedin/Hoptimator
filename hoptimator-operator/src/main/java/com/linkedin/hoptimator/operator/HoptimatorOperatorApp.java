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

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("Missing model file argument.");
    }
    HoptimatorPlanner.Factory plannerFactory = HoptimatorPlanner.Factory.fromModelFile(args[0],
        new Properties());

    // ensure model file works, and that static classes are initialized in the main thread
    HoptimatorPlanner planner = plannerFactory.makePlanner();

    ApiClient apiClient = Config.defaultClient();
    apiClient.setHttpClient(apiClient.getHttpClient().newBuilder()
      .readTimeout(0, TimeUnit.SECONDS).build());
    SharedInformerFactory informerFactory = new SharedInformerFactory(apiClient);
    Operator operator = new Operator("default", apiClient, informerFactory);
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
