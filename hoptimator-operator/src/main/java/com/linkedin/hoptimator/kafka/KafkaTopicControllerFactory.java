package com.linkedin.hoptimator.kafka;

import com.linkedin.hoptimator.Operator;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;

import java.util.function.Supplier;

public class KafkaTopicControllerFactory implements Supplier<Controller> {
  private final Operator operator;

  public static Controller create(Operator operator) {
    return (new KafkaTopicControllerFactory(operator)).get();
  }

  public KafkaTopicControllerFactory(Operator operator) {
    this.operator = operator;
  }

  public int workerCount() {
    return 1;
  }

  @Override
  public Controller get() {
    Reconciler reconciler = new KafkaTopicReconciler(operator);
    return ControllerBuilder.defaultBuilder(operator.informerFactory())
      .withReconciler(reconciler)
      .withName("kafka-topic-controller")
      .withWorkerCount(workerCount())
      //.withReadyFunc(resourceInformer::hasSynced) // optional, only starts controller when the
      // cache has synced up
      //.withWorkQueue(resourceWorkQueue)
      //.watch()
      .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1KafkaTopic.class, x).build())
      .build();
  }
}
