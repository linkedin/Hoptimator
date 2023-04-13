package com.linkedin.hoptimator;

import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.ControllerManager;

import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;
import com.linkedin.hoptimator.kafka.KafkaTopicControllerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HoptimatorOperatorApp {

  public static void main(String[] args) throws IOException {

    ApiClient apiClient = Config.defaultClient();
    apiClient.setHttpClient(apiClient.getHttpClient().newBuilder()
      .readTimeout(0, TimeUnit.SECONDS).build());
    SharedInformerFactory informerFactory = new SharedInformerFactory(apiClient);
    Operator operator = new Operator(apiClient, informerFactory);

    operator.registerApi("kafkatopic", "kafkatopics", "hoptimator.linkedin.com", "v1alpha1",
      V1alpha1KafkaTopic.class, V1alpha1KafkaTopicList.class);

    ControllerManager controllerManager = new ControllerManager(operator.informerFactory(),
      KafkaTopicControllerFactory.create(operator));
  
    controllerManager.run();  
  }
}
