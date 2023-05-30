package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.operator.ControllerProvider;
import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;

import java.util.Collection;
import java.util.Collections;

/** Provides a Controller plugin for KafkaTopics. */
public class KafkaControllerProvider implements ControllerProvider {

  @Override
  public Collection<Controller> controllers(Operator operator) {
    operator.registerApi("KafkaTopic", "kafkatopic", "kafkatopics", "hoptimator.linkedin.com",
      "v1alpha1", V1alpha1KafkaTopic.class, V1alpha1KafkaTopicList.class);

    Reconciler reconciler = new KafkaTopicReconciler(operator);
    Controller controller = ControllerBuilder.defaultBuilder(operator.informerFactory())
      .withReconciler(reconciler)
      .withName("kafka-topic-controller")
      .withWorkerCount(1)
      .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1KafkaTopic.class, x).build())
      .build();

    return Collections.singleton(controller);
  }
}
