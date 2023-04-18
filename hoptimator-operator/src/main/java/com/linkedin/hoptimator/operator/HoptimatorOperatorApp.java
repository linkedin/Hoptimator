package com.linkedin.hoptimator.operator;

import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;

import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;
import com.linkedin.hoptimator.models.V1alpha1Subscription;
import com.linkedin.hoptimator.models.V1alpha1SubscriptionList;
import com.linkedin.hoptimator.models.V1alpha1SqlJob;
import com.linkedin.hoptimator.models.V1alpha1SqlJobList;
import com.linkedin.hoptimator.operator.flink.FlinkSqlJobReconciler;
import com.linkedin.hoptimator.operator.kafka.KafkaTopicReconciler;
import com.linkedin.hoptimator.operator.subscription.SubscriptionReconciler;
import com.linkedin.hoptimator.planner.HoptimatorPlanner;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.Properties;

public class HoptimatorOperatorApp {

  public static void main(String[] args) throws Exception {
    String modelFile;
    if (args.length == 1) {
      modelFile = args[0];
    } else {
      modelFile = "../test-model.yaml";
    }
    HoptimatorPlanner.Factory plannerFactory = HoptimatorPlanner.Factory.fromModelFile(modelFile,
        new Properties());

    // ensure model file works, and that static classes are initialized in the main thread
    HoptimatorPlanner planner = plannerFactory.makePlanner();

    ApiClient apiClient = Config.defaultClient();
    apiClient.setHttpClient(apiClient.getHttpClient().newBuilder()
      .readTimeout(0, TimeUnit.SECONDS).build());
    SharedInformerFactory informerFactory = new SharedInformerFactory(apiClient);
    Operator operator = new Operator("default", apiClient, informerFactory);

    operator.registerApi("SqlJob", "sqljob", "sqljobs",
      "hoptimator.linkedin.com", "v1alpha1", V1alpha1SqlJob.class, V1alpha1SqlJobList.class);
 
    operator.registerApi("FlinkDeployment", "flinkdeployment", "flinkdeployments",
      "flink.apache.org", "v1beta1");
    
    operator.registerApi("KafkaTopic", "kafkatopic", "kafkatopics", "hoptimator.linkedin.com",
      "v1alpha1", V1alpha1KafkaTopic.class, V1alpha1KafkaTopicList.class);

    operator.registerApi("Subscription", "subscription", "subscriptions", "hoptimator.linkedin.com",
      "v1alpha1", V1alpha1Subscription.class, V1alpha1SubscriptionList.class);

    ControllerManager controllerManager = new ControllerManager(operator.informerFactory(),
      SubscriptionReconciler.controller(operator, plannerFactory),
      KafkaTopicReconciler.controller(operator),
      FlinkSqlJobReconciler.controller(operator));
  
    controllerManager.run();  
  }
}
