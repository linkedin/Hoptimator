package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.operator.ControllerProvider;
import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;
import com.linkedin.hoptimator.models.V1alpha1KafkaConnector;
import com.linkedin.hoptimator.models.V1alpha1KafkaConnectorList;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/** Provides a Controller plugin for KafkaTopics. */
public class KafkaControllerProvider implements ControllerProvider {

  @Override
  public Collection<Controller> controllers(Operator operator) {
    operator.registerApi("KafkaTopic", "kafkatopic", "kafkatopics", "hoptimator.linkedin.com",
      "v1alpha1", V1alpha1KafkaTopic.class, V1alpha1KafkaTopicList.class);

    operator.registerApi("KafkaConnector", "kafkaconnector", "kafkaconnectors", "hoptimator.linkedin.com",
      "v1alpha1", V1alpha1KafkaConnector.class, V1alpha1KafkaConnectorList.class);

    Reconciler topicReconciler = new KafkaTopicReconciler(operator);
    Controller topicController = ControllerBuilder.defaultBuilder(operator.informerFactory())
      .withReconciler(topicReconciler)
      .withName("kafka-topic-controller")
      .withWorkerCount(1)
      .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1KafkaTopic.class, x).build())
      .build();

    Reconciler connectorReconciler = new KafkaConnectorReconciler(operator);
    Controller connectorController = ControllerBuilder.defaultBuilder(operator.informerFactory())
      .withReconciler(connectorReconciler)
      .withName("kafka-connector-controller")
      .withWorkerCount(1)
      .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1KafkaConnector.class, x).build())
      .build();

    return Arrays.asList(new Controller[]{topicController, connectorController});
  }
}
