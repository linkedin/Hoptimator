package com.linkedin.hoptimator.operator.kafka;

import com.linkedin.hoptimator.operator.ControllerProvider;
import com.linkedin.hoptimator.operator.Operator;
import com.linkedin.hoptimator.models.V1alpha1Acl;
import com.linkedin.hoptimator.models.V1alpha1AclList;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopic;
import com.linkedin.hoptimator.models.V1alpha1KafkaTopicList;

import io.kubernetes.client.extended.controller.Controller;
import io.kubernetes.client.extended.controller.builder.ControllerBuilder;
import io.kubernetes.client.extended.controller.reconciler.Reconciler;

import java.util.Arrays;
import java.util.Collection;

/** Provides a Controller plugin for KafkaTopics. */
public class KafkaControllerProvider implements ControllerProvider {

  @Override
  public Collection<Controller> controllers(Operator operator) {
    operator.registerApi("KafkaTopic", "kafkatopic", "kafkatopics", "hoptimator.linkedin.com",
      "v1alpha1", V1alpha1KafkaTopic.class, V1alpha1KafkaTopicList.class);

    // N.B. this shared CRD may be re-registered by other ControllerProviders
    operator.registerApi("Acl", "acl", "acls", "hoptimator.linkedin.com",
      "v1alpha1", V1alpha1Acl.class, V1alpha1AclList.class);

    Reconciler topicReconciler = new KafkaTopicReconciler(operator);
    Controller topicController = ControllerBuilder.defaultBuilder(operator.informerFactory())
      .withReconciler(topicReconciler)
      .withName("kafka-topic-controller")
      .withWorkerCount(1)
      .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1KafkaTopic.class, x).build())
      .build();

    Reconciler topicAclReconciler = new KafkaTopicAclReconciler(operator);
    Controller topicAclController = ControllerBuilder.defaultBuilder(operator.informerFactory())
      .withReconciler(topicAclReconciler)
      .withName("kafka-topic-acl-controller")
      .withWorkerCount(1)
      .watch(x -> ControllerBuilder.controllerWatchBuilder(V1alpha1Acl.class, x).build())
      .build();

    return Arrays.asList(new Controller[]{topicController, topicAclController});
  }
}
